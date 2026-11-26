package com.aerosync.controller;

import com.aerosync.messaging.TelemetryStream;
import com.aerosync.model.FlightTelemetry;
import com.aerosync.observability.SystemMetricsService;
import com.aerosync.observability.TelemetryMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import com.aerosync.repository.FlightRecordRepository;
import com.aerosync.service.ActiveFlightService;
import com.aerosync.service.FlightLookupService;
import com.aerosync.service.TelemetryModeManager;
import com.aerosync.support.MutableClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.core.ReactiveHashOperations;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tools.jackson.databind.ObjectMapper;

import java.time.Clock;
import java.time.Duration;

import static com.aerosync.support.TestFlights.flight;
import static com.aerosync.support.TestFlights.record;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Contract tests for every public HTTP endpoint, error path and CORS rule. Controllers, services,
 * validation, the exception handler and CORS config are real; only Valkey, PostgreSQL and the mode
 * manager are mocked.
 */
@WebFluxTest(controllers = {FlightController.class, SystemController.class})
@Import({ActiveFlightService.class, FlightLookupService.class, TelemetryMetrics.class, SystemMetricsService.class,
        TelemetryStream.class, FlightControllerApiContractTest.ClockConfig.class})
@TestPropertySource(properties = {
        "aerosync.cors.allowed-origins=http://localhost:5173,https://*.vercel.app",
        "aerosync.cache.stale-seconds=300"
})
class FlightControllerApiContractTest {

    static final MutableClock CLOCK = MutableClock.at("2026-11-27T10:00:00Z");

    @TestConfiguration
    static class ClockConfig {
        @Bean
        @Primary
        Clock testClock() {
            return CLOCK;
        }

        @Bean
        MeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }
    }

    @Autowired
    WebTestClient web;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    TelemetryMetrics telemetryMetrics;

    @MockitoBean
    ReactiveStringRedisTemplate valkey;

    @MockitoBean
    FlightRecordRepository repository;

    @MockitoBean
    TelemetryModeManager modeManager;

    ReactiveHashOperations<String, String, String> hash;
    TelemetryModeManager realManager;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        CLOCK.set(java.time.Instant.parse("2026-11-27T10:00:00Z"));
        hash = org.mockito.Mockito.mock(ReactiveHashOperations.class);
        when(valkey.<String, String>opsForHash()).thenReturn(hash);
        when(hash.values(ActiveFlightService.ACTIVE_FLIGHTS_KEY)).thenReturn(Flux.empty());
        when(repository.findLatestByIdentifier(anyString())).thenReturn(Mono.empty());
        when(repository.findLatestSince(anyLong())).thenReturn(Flux.empty());

        realManager = new TelemetryModeManager(true, 400, 3, 900_000, CLOCK);
        when(modeManager.getStatus(anyLong())).thenAnswer(inv -> realManager.getStatus(inv.getArgument(0)));
    }

    private String json(FlightTelemetry f) {
        return objectMapper.writeValueAsString(f);
    }

    private long now() {
        return CLOCK.millis();
    }

    private void cacheContains(String... values) {
        when(hash.values(ActiveFlightService.ACTIVE_FLIGHTS_KEY)).thenReturn(Flux.just(values));
    }

    private void assertApiError(WebTestClient.ResponseSpec spec, int status, String error, String path) {
        spec.expectStatus().isEqualTo(status)
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_JSON)
                .expectBody()
                .jsonPath("$.status").isEqualTo(status)
                .jsonPath("$.error").isEqualTo(error)
                .jsonPath("$.message").isNotEmpty()
                .jsonPath("$.path").isEqualTo(path)
                .jsonPath("$.timestamp").isEqualTo("2026-11-27T10:00:00Z")
                .jsonPath("$.trace").doesNotExist()
                .jsonPath("$.exception").doesNotExist();
    }

    // ------------------------------------------------------------------ health & status

    @Nested
    class HealthAndStatus {

        @Test
        void healthIsUp() {
            web.get().uri("/api/health").exchange()
                    .expectStatus().isOk()
                    .expectBody()
                    .jsonPath("$.status").isEqualTo("UP")
                    .jsonPath("$.service").isEqualTo("aerosync-backend")
                    .jsonPath("$.timestamp").isEqualTo("2026-11-27T10:00:00Z");
            verifyNoInteractions(valkey, repository);
        }

        @Test
        void liveStatusContractCountsOnlyFreshFlights() {
            realManager.tryAcquireLiveRequest();
            realManager.tryAcquireLiveRequest();
            cacheContains(
                    json(flight("AIC860", "800a01", now())),
                    json(flight("IGO612", "800b02", now() - 10_000)),
                    json(flight("OLD001", "abcdef", now() - Duration.ofMinutes(10).toMillis())));

            web.get().uri("/api/system/status").exchange()
                    .expectStatus().isOk()
                    .expectBody()
                    .jsonPath("$.mode").isEqualTo("LIVE")
                    .jsonPath("$.reason").isEqualTo("NONE")
                    .jsonPath("$.message").isEqualTo("Live OpenSky radar active (2/400 polls used today).")
                    .jsonPath("$.requestsUsedToday").isEqualTo(2)
                    .jsonPath("$.dailyLimit").isEqualTo(400)
                    .jsonPath("$.windowResetAt").isEqualTo("2026-11-28T00:00:00Z")
                    .jsonPath("$.activeFlightCount").isEqualTo(2)
                    .jsonPath("$.feed").isEqualTo("LIVE_FEED");
        }

        @Test
        void simulationStatusContractAfterQuotaExhaustion() {
            realManager = new TelemetryModeManager(true, 3, 3, 900_000, CLOCK);
            for (int i = 0; i < 4; i++) {
                realManager.tryAcquireLiveRequest();
            }
            cacheContains(json(flight("AIC860", "800a01", now())));

            web.get().uri("/api/system/status").exchange()
                    .expectStatus().isOk()
                    .expectBody()
                    .jsonPath("$.mode").isEqualTo("SIMULATION")
                    .jsonPath("$.reason").isEqualTo("DAILY_QUOTA_EXHAUSTED")
                    .jsonPath("$.message").isEqualTo("Free-tier daily OpenSky quota reached (3/3). Live flight simulator active.")
                    .jsonPath("$.requestsUsedToday").isEqualTo(3)
                    .jsonPath("$.dailyLimit").isEqualTo(3)
                    .jsonPath("$.activeFlightCount").isEqualTo(1)
                    .jsonPath("$.feed").isEqualTo("SIMULATED_FALLBACK");
        }

        @Test
        void simulationStatusContractAfterRateLimit() {
            realManager.recordUpstreamRateLimit();

            web.get().uri("/api/system/status").exchange()
                    .expectStatus().isOk()
                    .expectBody()
                    .jsonPath("$.mode").isEqualTo("SIMULATION")
                    .jsonPath("$.reason").isEqualTo("UPSTREAM_RATE_LIMITED_429");
        }

        @Test
        void metricsSnapshotContractAndCacheHitRatio() {
            // The meter registry is shared across this test class, so assert on deltas.
            long hitsBefore = telemetryMetrics.lookupHits();
            long missesBefore = telemetryMetrics.lookupMisses();
            long lookupsBefore = hitsBefore + missesBefore + telemetryMetrics.lookupErrors();
            cacheContains(json(flight("AIC860", "800a01", now())));
            web.get().uri("/api/flights/AIC860").exchange().expectStatus().isOk();       // Valkey hit
            web.get().uri("/api/flights/ZZZ999").exchange().expectStatus().isNotFound(); // miss -> PostgreSQL

            assertThat(telemetryMetrics.lookupHits()).isEqualTo(hitsBefore + 1);
            assertThat(telemetryMetrics.lookupMisses()).isEqualTo(missesBefore + 1);
            web.get().uri("/api/system/metrics").exchange()
                    .expectStatus().isOk()
                    .expectBody()
                    .jsonPath("$.feed").isEqualTo("LIVE_FEED")
                    .jsonPath("$.cacheLookups").isEqualTo((int) (lookupsBefore + 2))
                    .jsonPath("$.cacheHitRatio").isNumber()
                    .jsonPath("$.eventsPerSecond").isEqualTo(0.0)
                    .jsonPath("$.websocketSubscribers").isEqualTo(0)
                    .jsonPath("$.upstreamErrors").isEqualTo(0)
                    .jsonPath("$.uptimeSeconds").exists()
                    .jsonPath("$.timestamp").isEqualTo("2026-11-27T10:00:00Z");
        }

        @Test
        void valkeyErrorCountsAsLookupErrorNotMiss() {
            long missesBefore = telemetryMetrics.lookupMisses();
            long errorsBefore = telemetryMetrics.lookupErrors();
            when(hash.values(ActiveFlightService.ACTIVE_FLIGHTS_KEY)).thenReturn(Flux.error(new IllegalStateException("down")));

            web.get().uri("/api/flights/AIC860").exchange().expectStatus().isNotFound();

            assertThat(telemetryMetrics.lookupErrors()).isEqualTo(errorsBefore + 1);
            assertThat(telemetryMetrics.lookupMisses()).isEqualTo(missesBefore);
        }

        @Test
        void statusDegradesToZeroFlightsWhenValkeyIsDown() {
            when(hash.values(ActiveFlightService.ACTIVE_FLIGHTS_KEY))
                    .thenReturn(Flux.error(new IllegalStateException("Unable to connect to Redis")));

            web.get().uri("/api/system/status").exchange()
                    .expectStatus().isOk()
                    .expectBody()
                    .jsonPath("$.mode").isEqualTo("LIVE")
                    .jsonPath("$.activeFlightCount").isEqualTo(0);
        }

        @Test
        void unexpectedFailureIsSanitised500() {
            when(modeManager.getStatus(anyLong()))
                    .thenThrow(new IllegalStateException("redis://admin:hunter2@10.0.0.5:6379 at com.aerosync.Secret"));

            WebTestClient.ResponseSpec spec = web.get().uri("/api/system/status").exchange();
            spec.expectBody(String.class).value(body -> assertThat(body).doesNotContain("hunter2"));
            assertApiError(web.get().uri("/api/system/status").exchange(), 500, "INTERNAL_ERROR", "/api/system/status");
        }
    }

    // ------------------------------------------------------------------ active flights

    @Nested
    class ActiveFlights {

        @Test
        void returnsFreshFlightsSortedAndExcludesStaleOnes() {
            cacheContains(
                    json(flight("IGO612", "800b02", now())),
                    json(flight("AIC860", "800a01", now() - 1000)),
                    json(flight("STALE1", "aaaaaa", now() - Duration.ofSeconds(301).toMillis())));

            web.get().uri("/api/flights/active").exchange()
                    .expectStatus().isOk()
                    .expectBody()
                    .jsonPath("$.length()").isEqualTo(2)
                    .jsonPath("$[0].callsign").isEqualTo("AIC860")
                    .jsonPath("$[1].callsign").isEqualTo("IGO612")
                    .jsonPath("$[0].icao24").isEqualTo("800a01")
                    .jsonPath("$[0].altitude").isEqualTo(35000.0)
                    .jsonPath("$[0].heading").isEqualTo(210.0);
        }

        @Test
        void liveFeedHidesSimulatorLeftoversAndSimulatedFeedHidesLiveData() {
            cacheContains(
                    json(flight("ABY800", "896558", now())),
                    json(com.aerosync.support.TestFlights.simulated("AIC860", "800a01", now())));

            when(modeManager.isSimulationMode()).thenReturn(false);
            web.get().uri("/api/flights/active").exchange()
                    .expectStatus().isOk()
                    .expectBody()
                    .jsonPath("$.length()").isEqualTo(1)
                    .jsonPath("$[0].callsign").isEqualTo("ABY800")
                    .jsonPath("$[0].origin").isEqualTo("OPENSKY");
            // A simulator leftover is not reported as an ACTIVE live flight.
            web.get().uri("/api/flights/AIC860").exchange().expectStatus().isNotFound();

            when(modeManager.isSimulationMode()).thenReturn(true);
            web.get().uri("/api/flights/active").exchange()
                    .expectStatus().isOk()
                    .expectBody()
                    .jsonPath("$.length()").isEqualTo(1)
                    .jsonPath("$[0].callsign").isEqualTo("AIC860")
                    .jsonPath("$[0].origin").isEqualTo("SIMULATOR");
        }

        @Test
        void untaggedLegacyEntriesCountAsLiveData() {
            cacheContains("{\"callsign\":\"OLD123\",\"icao24\":\"abcdef\",\"latitude\":1,\"longitude\":2,"
                    + "\"altitude\":3,\"velocity\":4,\"heading\":5,\"timestamp\":" + now() + "}");
            web.get().uri("/api/flights/active").exchange()
                    .expectStatus().isOk()
                    .expectBody().jsonPath("$[0].callsign").isEqualTo("OLD123");
        }

        @Test
        void emptyCacheReturnsEmptyArray() {
            web.get().uri("/api/flights/active").exchange()
                    .expectStatus().isOk()
                    .expectBody().json("[]");
        }

        @Test
        void corruptedEntriesAreSkippedWithoutFailingTheResponse() {
            cacheContains(
                    "{not json",
                    json(flight("AIC860", "800a01", now())),
                    "{\"callsign\":null,\"timestamp\":1}",
                    "[]",
                    "{\"callsign\":\"BAD\",\"latitude\":\"north\"}");

            web.get().uri("/api/flights/active").exchange()
                    .expectStatus().isOk()
                    .expectBody()
                    .jsonPath("$.length()").isEqualTo(1)
                    .jsonPath("$[0].callsign").isEqualTo("AIC860");
        }

        @Test
        void valkeyOutageFallsBackToRecentPostgresPositions() {
            when(hash.values(ActiveFlightService.ACTIVE_FLIGHTS_KEY)).thenReturn(Flux.error(new IllegalStateException("down")));
            when(repository.findLatestSince(anyLong())).thenReturn(Flux.just(record("VTI941", "800c03", now() - 5000)));

            web.get().uri("/api/flights/active").exchange()
                    .expectStatus().isOk()
                    .expectBody()
                    .jsonPath("$.length()").isEqualTo(1)
                    .jsonPath("$[0].callsign").isEqualTo("VTI941");
            verify(repository).findLatestSince(now() - 300_000);
        }

        @Test
        void bothStoresDownIs503() {
            when(hash.values(ActiveFlightService.ACTIVE_FLIGHTS_KEY)).thenReturn(Flux.error(new IllegalStateException("down")));
            when(repository.findLatestSince(anyLong()))
                    .thenReturn(Flux.error(new IllegalStateException("FATAL: password authentication failed for user aerosync")));

            WebTestClient.ResponseSpec spec = web.get().uri("/api/flights/active").exchange();
            assertApiError(spec, 503, "SERVICE_UNAVAILABLE", "/api/flights/active");
            web.get().uri("/api/flights/active").exchange()
                    .expectBody(String.class).value(body -> assertThat(body).doesNotContain("password"));
        }
    }

    // ------------------------------------------------------------------ lookup

    @Nested
    class Lookup {

        @Test
        void callsignHitInValkeyIsCaseInsensitive() {
            cacheContains(json(flight("IGO612", "800b02", now())), json(flight("AIC860", "800a01", now())));

            web.get().uri("/api/flights/aic860").exchange()
                    .expectStatus().isOk()
                    .expectBody()
                    .jsonPath("$.callsign").isEqualTo("AIC860")
                    .jsonPath("$.icao24").isEqualTo("800a01")
                    .jsonPath("$.status").isEqualTo("ACTIVE")
                    .jsonPath("$.source").isEqualTo("VALKEY_CACHE")
                    .jsonPath("$.latitude").isEqualTo(28.55)
                    .jsonPath("$.longitude").isEqualTo(77.10)
                    .jsonPath("$.altitude").isEqualTo(35000.0)
                    .jsonPath("$.velocity").isEqualTo(245.0)
                    .jsonPath("$.velocityKnots").isEqualTo(476.2)
                    .jsonPath("$.heading").isEqualTo(210.0)
                    .jsonPath("$.timestamp").isEqualTo(now())
                    .jsonPath("$.origin").isEqualTo("OPENSKY");
            verify(repository, never()).findLatestByIdentifier(anyString());
        }

        @Test
        void icao24HitInValkeyIsCaseInsensitive() {
            cacheContains(json(flight("AIC860", "800a01", now())));

            web.get().uri("/api/flights/800A01").exchange()
                    .expectStatus().isOk()
                    .expectBody()
                    .jsonPath("$.callsign").isEqualTo("AIC860")
                    .jsonPath("$.source").isEqualTo("VALKEY_CACHE");
        }

        @Test
        void surroundingWhitespaceIsTrimmed() {
            cacheContains(json(flight("AIC860", "800a01", now())));

            web.get().uri("/api/flights/{id}", "  AIC860 ").exchange()
                    .expectStatus().isOk()
                    .expectBody().jsonPath("$.callsign").isEqualTo("AIC860");
        }

        @Test
        void valkeyMissFallsBackToPostgresArchive() {
            when(repository.findLatestByIdentifier("SIA423")).thenReturn(Mono.just(record("SIA423", "801108", 1_700_000_000_000L)));

            web.get().uri("/api/flights/SIA423").exchange()
                    .expectStatus().isOk()
                    .expectBody()
                    .jsonPath("$.callsign").isEqualTo("SIA423")
                    .jsonPath("$.status").isEqualTo("HISTORICAL")
                    .jsonPath("$.source").isEqualTo("POSTGRES_ARCHIVE")
                    .jsonPath("$.timestamp").isEqualTo(1_700_000_000_000L)
                    .jsonPath("$.velocityKnots").isEqualTo(349.9);
        }

        @Test
        void staleCacheEntryIsNotReportedActive() {
            cacheContains(json(flight("AIC860", "800a01", now() - Duration.ofHours(1).toMillis())));
            when(repository.findLatestByIdentifier("AIC860")).thenReturn(Mono.just(record("AIC860", "800a01", now() - 3_600_000)));

            web.get().uri("/api/flights/AIC860").exchange()
                    .expectStatus().isOk()
                    .expectBody().jsonPath("$.status").isEqualTo("HISTORICAL");
        }

        @Test
        void valkeyErrorFallsBackToPostgresHit() {
            when(hash.values(ActiveFlightService.ACTIVE_FLIGHTS_KEY)).thenReturn(Flux.error(new IllegalStateException("down")));
            when(repository.findLatestByIdentifier("AIC860")).thenReturn(Mono.just(record("AIC860", "800a01", 5L)));

            web.get().uri("/api/flights/AIC860").exchange()
                    .expectStatus().isOk()
                    .expectBody()
                    .jsonPath("$.source").isEqualTo("POSTGRES_ARCHIVE");
        }

        @Test
        void valkeyErrorAndPostgresMissIs404NotA500() {
            when(hash.values(ActiveFlightService.ACTIVE_FLIGHTS_KEY)).thenReturn(Flux.error(new IllegalStateException("down")));

            assertApiError(web.get().uri("/api/flights/AIC860").exchange(), 404, "FLIGHT_NOT_FOUND", "/api/flights/AIC860");
        }

        @Test
        void missingEverywhereIsStructured404() {
            web.get().uri("/api/flights/ZZZ999").exchange()
                    .expectStatus().isNotFound()
                    .expectBody()
                    .jsonPath("$.status").isEqualTo(404)
                    .jsonPath("$.error").isEqualTo("FLIGHT_NOT_FOUND")
                    .jsonPath("$.message").isEqualTo("No flight found for 'ZZZ999'.")
                    .jsonPath("$.path").isEqualTo("/api/flights/ZZZ999")
                    .jsonPath("$.timestamp").isEqualTo("2026-11-27T10:00:00Z");
        }

        @Test
        void postgresFailureAfterCacheMissIs503() {
            when(repository.findLatestByIdentifier(anyString()))
                    .thenReturn(Mono.error(new IllegalStateException("relation \"flight_telemetry_history\" does not exist")));

            assertApiError(web.get().uri("/api/flights/AIC860").exchange(), 503, "SERVICE_UNAVAILABLE", "/api/flights/AIC860");
            web.get().uri("/api/flights/AIC860").exchange()
                    .expectBody(String.class).value(body -> assertThat(body).doesNotContain("relation"));
        }

        @ParameterizedTest
        @ValueSource(strings = {
                "   ",
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456",   // 33 chars
                "' OR 1=1--",
                "<script>alert(1)</script>",
                "AIC.860",
                "AIC860;DROP TABLE flight_telemetry_history",
                "%00",
                "飞机"
        })
        void invalidIdentifiersAreRejectedWith400BeforeTouchingAnyStore(String identifier) {
            web.get().uri("/api/flights/{id}", identifier).exchange()
                    .expectStatus().isBadRequest()
                    .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_JSON)
                    .expectBody()
                    .jsonPath("$.status").isEqualTo(400)
                    .jsonPath("$.error").isEqualTo("INVALID_IDENTIFIER")
                    .jsonPath("$.message").isNotEmpty()
                    .jsonPath("$.timestamp").isEqualTo("2026-11-27T10:00:00Z");
            verifyNoInteractions(repository);
            verify(hash, never()).values(anyString());
        }

        @Test
        void exactly32CharactersIsAccepted() {
            web.get().uri("/api/flights/{id}", "A".repeat(32)).exchange()
                    .expectStatus().isNotFound();
        }
    }

    // ------------------------------------------------------------------ routing safety

    @Nested
    class Routing {

        @Test
        void postToActiveFlightsIs405() {
            WebTestClient.ResponseSpec spec = web.post().uri("/api/flights/active").exchange();
            spec.expectHeader().valueMatches(HttpHeaders.ALLOW, ".*GET.*");
            assertApiError(web.post().uri("/api/flights/active").exchange(), 405, "METHOD_NOT_ALLOWED", "/api/flights/active");
        }

        @Test
        void deleteFlightIs405() {
            assertApiError(web.delete().uri("/api/flights/AIC860").exchange(), 405, "METHOD_NOT_ALLOWED", "/api/flights/AIC860");
        }

        @Test
        void putHealthIs405() {
            assertApiError(web.put().uri("/api/health").exchange(), 405, "METHOD_NOT_ALLOWED", "/api/health");
        }

        @Test
        void unknownApiPathIs404() {
            assertApiError(web.get().uri("/api/does-not-exist").exchange(), 404, "NOT_FOUND", "/api/does-not-exist");
        }

        @Test
        void trailingSlashWithoutIdentifierIs404() {
            assertApiError(web.get().uri("/api/flights/").exchange(), 404, "NOT_FOUND", "/api/flights/");
        }

        @Test
        void nestedUnknownPathIs404() {
            assertApiError(web.get().uri("/api/flights/AIC860/trail/extra").exchange(), 404, "NOT_FOUND",
                    "/api/flights/AIC860/trail/extra");
        }
    }

    // ------------------------------------------------------------------ CORS

    @Nested
    class Cors {

        /** Absolute base URL: Spring rejects cross-origin checks on host-less mock requests. */
        WebTestClient api() {
            return web.mutate().baseUrl("http://api.aerosync.test").build();
        }

        @Test
        void allowedOriginPreflightSucceeds() {
            api().options().uri("/api/flights/active")
                    .header(HttpHeaders.ORIGIN, "http://localhost:5173")
                    .header(HttpHeaders.ACCESS_CONTROL_REQUEST_METHOD, "GET")
                    .exchange()
                    .expectStatus().isOk()
                    .expectHeader().valueEquals(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN, "http://localhost:5173");
        }

        @Test
        void wildcardPatternOriginPreflightSucceeds() {
            api().options().uri("/api/system/status")
                    .header(HttpHeaders.ORIGIN, "https://aerosync-git-main.vercel.app")
                    .header(HttpHeaders.ACCESS_CONTROL_REQUEST_METHOD, "GET")
                    .exchange()
                    .expectStatus().isOk()
                    .expectHeader().valueEquals(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN, "https://aerosync-git-main.vercel.app");
        }

        @Test
        void disallowedOriginPreflightIsForbidden() {
            api().options().uri("/api/flights/active")
                    .header(HttpHeaders.ORIGIN, "https://unauthorized.example")
                    .header(HttpHeaders.ACCESS_CONTROL_REQUEST_METHOD, "GET")
                    .exchange()
                    .expectStatus().isForbidden()
                    .expectHeader().doesNotExist(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN);
        }

        @Test
        void disallowedMethodPreflightIsForbidden() {
            api().options().uri("/api/flights/active")
                    .header(HttpHeaders.ORIGIN, "http://localhost:5173")
                    .header(HttpHeaders.ACCESS_CONTROL_REQUEST_METHOD, "DELETE")
                    .exchange()
                    .expectStatus().isForbidden();
        }

        @Test
        void simpleGetFromAllowedOriginCarriesCorsHeader() {
            api().get().uri("/api/health")
                    .header(HttpHeaders.ORIGIN, "http://localhost:5173")
                    .exchange()
                    .expectStatus().isOk()
                    .expectHeader().valueEquals(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN, "http://localhost:5173");
        }

        @Test
        void simpleGetFromDisallowedOriginIsForbidden() {
            api().method(HttpMethod.GET).uri("/api/health")
                    .header(HttpHeaders.ORIGIN, "https://unauthorized.example")
                    .exchange()
                    .expectStatus().isForbidden();
        }
    }
}
