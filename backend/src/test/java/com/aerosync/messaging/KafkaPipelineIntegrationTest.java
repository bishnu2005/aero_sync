package com.aerosync.messaging;

import com.aerosync.model.FlightRecord;
import com.aerosync.model.FlightTelemetry;
import com.aerosync.repository.FlightRecordRepository;
import com.aerosync.service.ActiveFlightService;
import com.aerosync.service.TelemetryModeManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.data.redis.core.ReactiveHashOperations;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.http.HttpHeaders;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.net.URI;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Full application context on a random port with an in-memory Kafka broker. Proves the real
 * producer -> Kafka -> consumer -> Valkey / PostgreSQL / WebSocket path. No Docker, and OpenSky is
 * disabled (plus pointed at an unroutable address), so zero live OpenSky calls.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.sql.init.mode=never",
        "aerosync.opensky.enabled=false",
        "aerosync.opensky.api-url=http://127.0.0.1:9/blocked-in-tests",
        "aerosync.simulator.initial-delay-ms=3600000",
        "aerosync.db.prune-initial-delay-ms=3600000",
        "aerosync.cache.eviction-initial-delay-ms=3600000",
        "aerosync.cors.allowed-origins=http://localhost:5173",
        // Valkey/PostgreSQL are mocked here, so their connection-level health checks are switched off.
        "management.health.redis.enabled=false",
        "management.health.r2dbc.enabled=false"
})
@EmbeddedKafka(partitions = 1, topics = TelemetryProducer.TOPIC)
class KafkaPipelineIntegrationTest {

    @LocalServerPort
    int port;

    @Autowired
    TelemetryProducer producer;

    @Autowired
    TelemetryModeManager modeManager;

    @MockitoBean
    ReactiveStringRedisTemplate valkey;

    @MockitoBean
    FlightRecordRepository repository;

    ReactiveHashOperations<String, String, String> hash;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        hash = mock(ReactiveHashOperations.class);
        when(valkey.<String, String>opsForHash()).thenReturn(hash);
        when(hash.put(anyString(), anyString(), anyString())).thenReturn(Mono.just(true));
        when(repository.save(any(FlightRecord.class))).thenAnswer(inv -> Mono.just(inv.getArgument(0)));
    }

    @Test
    void openSkyDisabledBootsInSimulationMode() {
        assertThat(modeManager.isSimulationMode()).isTrue();
        assertThat(modeManager.getReason()).isEqualTo(TelemetryModeManager.FailoverReason.MANUAL_SIMULATION_CONFIG);
    }

    @Test
    void telemetryFlowsFromKafkaToValkeyPostgresAndWebSocket() {
        Sinks.One<String> received = Sinks.one();
        Sinks.Empty<Void> connected = Sinks.empty();
        ReactorNettyWebSocketClient client = new ReactorNettyWebSocketClient();
        HttpHeaders headers = new HttpHeaders();
        headers.setOrigin("http://localhost:5173");

        var session = client.execute(URI.create("ws://localhost:" + port + "/ws/telemetry"), headers, s -> {
                    connected.tryEmitEmpty();
                    return s.receive()
                            .map(WebSocketMessage::getPayloadAsText)
                            .filter(text -> text.contains("ITEST1"))
                            .next()
                            .doOnNext(received::tryEmitValue)
                            .then();
                })
                .subscribe();
        connected.asMono().block(Duration.ofSeconds(10));

        FlightTelemetry flight = new FlightTelemetry(" ITEST1 ", "abc123", 20.0, 78.0, 33000, 230, 45, System.currentTimeMillis(),
                FlightTelemetry.ORIGIN_SIMULATOR);
        // Retry until the consumer group has its partition assignment.
        String frame = null;
        for (int attempt = 0; attempt < 10 && frame == null; attempt++) {
            producer.publish(flight).block(Duration.ofSeconds(10));
            frame = received.asMono().block(Duration.ofSeconds(3));
        }
        session.dispose();

        assertThat(frame).contains("\"callsign\":\"ITEST1\"").contains("\"icao24\":\"abc123\"");
        verify(hash, timeout(10_000).atLeastOnce())
                .put(eq(ActiveFlightService.ACTIVE_FLIGHTS_KEY), eq("ITEST1"), anyString());
        verify(repository, timeout(10_000).atLeastOnce()).save(any(FlightRecord.class));

        // The same event is visible to observability.
        WebTestClient http = WebTestClient.bindToServer().baseUrl("http://localhost:" + port).build();
        http.get().uri("/api/system/metrics").exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.feed").isEqualTo("SIMULATED_MANUAL")
                .jsonPath("$.eventsConsumed").value(v -> assertThat(((Number) v).longValue()).isPositive());
        http.get().uri("/actuator/metrics/aerosync.telemetry.consumed").exchange()
                .expectStatus().isOk()
                .expectBody().jsonPath("$.measurements[0].value").value(v -> assertThat(((Number) v).doubleValue()).isPositive());
    }

    @Test
    void actuatorHealthProbesAndPrometheusAreExposed() {
        WebTestClient http = WebTestClient.bindToServer().baseUrl("http://localhost:" + port).build();
        http.get().uri("/actuator/health").exchange()
                .expectStatus().isOk()
                .expectBody().jsonPath("$.status").isEqualTo("UP");
        http.get().uri("/actuator/health/liveness").exchange().expectStatus().isOk();
        http.get().uri("/actuator/health/readiness").exchange().expectStatus().isOk();
        http.get().uri("/actuator/prometheus").exchange()
                .expectStatus().isOk()
                .expectBody(String.class).value(body -> assertThat(body)
                        .contains("aerosync_telemetry_consumed_total")
                        .contains("aerosync_websocket_subscribers")
                        .contains("aerosync_opensky_quota_limit")
                        .contains("aerosync_mode_simulation")
                        .contains("application=\"aerosync-backend\""));
        http.get().uri("/actuator/env").exchange().expectStatus().isNotFound(); // not exposed
    }

    @Test
    void webSocketHandshakeFromDisallowedOriginIsRejected() {
        ReactorNettyWebSocketClient client = new ReactorNettyWebSocketClient();
        HttpHeaders headers = new HttpHeaders();
        headers.setOrigin("https://unauthorized.example");

        assertThatThrownBy(() -> client.execute(URI.create("ws://localhost:" + port + "/ws/telemetry"), headers,
                        s -> s.receive().then())
                .block(Duration.ofSeconds(10)))
                .hasMessageContaining("403");
    }
}
