package com.aerosync.messaging;

import com.aerosync.model.FlightRecord;
import com.aerosync.model.FlightTelemetry;
import com.aerosync.observability.TelemetryMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import com.aerosync.repository.FlightRecordRepository;
import com.aerosync.service.ActiveFlightService;
import com.aerosync.support.MutableClock;
import com.aerosync.support.TestFlights;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.core.ReactiveHashOperations;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import reactor.core.publisher.Mono;

import java.time.Duration;

import static com.aerosync.support.TestFlights.flight;
import static com.aerosync.support.TestFlights.json;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class TelemetryConsumerTest {

    private final MutableClock clock = MutableClock.at("2026-11-27T10:00:00Z");
    private TelemetryStream stream;
    private FlightRecordRepository repository;
    private ReactiveStringRedisTemplate valkey;
    private ReactiveHashOperations<String, String, String> hash;
    private TelemetryConsumer consumer;
    private TelemetryMetrics metrics;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        stream = mock(TelemetryStream.class);
        repository = mock(FlightRecordRepository.class);
        valkey = mock(ReactiveStringRedisTemplate.class);
        hash = mock(ReactiveHashOperations.class);
        when(valkey.<String, String>opsForHash()).thenReturn(hash);
        when(hash.put(anyString(), anyString(), anyString())).thenReturn(Mono.just(true));
        when(repository.save(any(FlightRecord.class))).thenAnswer(inv -> Mono.just(inv.getArgument(0)));
        metrics = new TelemetryMetrics(new SimpleMeterRegistry());
        consumer = new TelemetryConsumer(stream, repository, valkey, TestFlights.MAPPER, clock, metrics, 15);
    }

    private String message(String callsign) {
        return json(flight(callsign, "800a01", clock.millis()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "   ", "\t\n"})
    void blankCallsignsAreDroppedEverywhere(String callsign) {
        consumer.consume(message(callsign));

        verifyNoInteractions(valkey, repository, stream);
    }

    @Test
    void nullCallsignIsDroppedEverywhere() {
        consumer.consume("{\"callsign\":null,\"icao24\":\"800a01\",\"latitude\":1,\"longitude\":2,"
                + "\"altitude\":3,\"velocity\":4,\"heading\":5,\"timestamp\":6}");

        verifyNoInteractions(valkey, repository, stream);
    }

    @Test
    void validMessageIsCachedPersistedAndBroadcastWithTrimmedCallsign() {
        consumer.consume(message("  AIC860  "));

        ArgumentCaptor<String> cached = ArgumentCaptor.forClass(String.class);
        verify(hash).put(eq(ActiveFlightService.ACTIVE_FLIGHTS_KEY), eq("AIC860"), cached.capture());
        assertThat(TestFlights.MAPPER.readValue(cached.getValue(), FlightTelemetry.class).callsign()).isEqualTo("AIC860");

        ArgumentCaptor<FlightRecord> saved = ArgumentCaptor.forClass(FlightRecord.class);
        verify(repository).save(saved.capture());
        assertThat(saved.getValue().callsign()).isEqualTo("AIC860");
        assertThat(saved.getValue().icao24()).isEqualTo("800a01");

        verify(stream).broadcast(cached.getValue());
    }

    @Test
    void postgresWritesAreThrottledPerCallsignButCacheAndStreamAreNot() {
        consumer.consume(message("AIC860"));
        clock.advance(Duration.ofSeconds(3));
        consumer.consume(message("AIC860"));

        verify(hash, times(2)).put(eq(ActiveFlightService.ACTIVE_FLIGHTS_KEY), eq("AIC860"), anyString());
        verify(stream, times(2)).broadcast(anyString());
        verify(repository, times(1)).save(any());

        clock.advance(Duration.ofSeconds(12));
        consumer.consume(message("AIC860"));
        verify(repository, times(2)).save(any());

        assertThat(metrics.consumedTotal()).isEqualTo(3);
        assertThat(metrics.dbWrites()).isEqualTo(2);
        assertThat(metrics.dbWritesThrottled()).isEqualTo(1);
    }

    @Test
    void dropsAndFailuresAreCounted() {
        when(hash.put(anyString(), anyString(), anyString())).thenReturn(Mono.error(new IllegalStateException("down")));
        when(repository.save(any(FlightRecord.class))).thenReturn(Mono.error(new IllegalStateException("down")));

        consumer.consume("not json");
        consumer.consume(message("  "));
        consumer.consume(message("AIC860"));

        assertThat(metrics.droppedTotal()).isEqualTo(2);
        assertThat(metrics.consumedTotal()).isEqualTo(1);
        assertThat(metrics.cacheWriteFailures()).isEqualTo(1);
        assertThat(metrics.dbWriteFailures()).isEqualTo(1);
    }

    @Test
    void throttleIsIndependentPerCallsign() {
        consumer.consume(message("AIC860"));
        consumer.consume(message("IGO612"));

        verify(repository, times(2)).save(any());
    }

    @Test
    void valkeyErrorDoesNotBlockPostgresOrBroadcast() {
        when(hash.put(anyString(), anyString(), anyString())).thenReturn(Mono.error(new IllegalStateException("valkey down")));

        assertThatCode(() -> consumer.consume(message("AIC860"))).doesNotThrowAnyException();

        verify(repository).save(any());
        verify(stream).broadcast(anyString());
    }

    @Test
    void valkeyThrowingSynchronouslyDoesNotBlockPostgresOrBroadcast() {
        when(valkey.<String, String>opsForHash()).thenThrow(new IllegalStateException("connection pool exhausted"));

        assertThatCode(() -> consumer.consume(message("AIC860"))).doesNotThrowAnyException();

        verify(repository).save(any());
        verify(stream).broadcast(anyString());
    }

    @Test
    void postgresErrorDoesNotBlockValkeyOrBroadcastAndRetriesNextMessage() {
        when(repository.save(any(FlightRecord.class))).thenReturn(Mono.error(new IllegalStateException("db down")));

        assertThatCode(() -> consumer.consume(message("AIC860"))).doesNotThrowAnyException();

        verify(hash).put(eq(ActiveFlightService.ACTIVE_FLIGHTS_KEY), eq("AIC860"), anyString());
        verify(stream).broadcast(anyString());

        // A failed save releases the throttle slot, so the very next message retries the write.
        consumer.consume(message("AIC860"));
        verify(repository, times(2)).save(any());
    }

    @Test
    void postgresThrowingSynchronouslyDoesNotBlockValkeyOrBroadcast() {
        when(repository.save(any(FlightRecord.class))).thenThrow(new IllegalStateException("pool closed"));

        assertThatCode(() -> consumer.consume(message("AIC860"))).doesNotThrowAnyException();

        verify(hash).put(anyString(), anyString(), anyString());
        verify(stream).broadcast(anyString());
    }

    @Test
    void broadcastFailureDoesNotEscapeTheListener() {
        org.mockito.Mockito.doThrow(new IllegalStateException("sink closed")).when(stream).broadcast(anyString());

        assertThatCode(() -> consumer.consume(message("AIC860"))).doesNotThrowAnyException();
        verify(hash).put(anyString(), anyString(), anyString());
        verify(repository).save(any());
    }

    @ParameterizedTest
    @ValueSource(strings = {"not json", "{\"callsign\": \"AIC860\", ", "[]", "42", "{\"callsign\":\"X\",\"latitude\":\"north\"}"})
    void malformedJsonIsSwallowed(String payload) {
        assertThatCode(() -> consumer.consume(payload)).doesNotThrowAnyException();

        verifyNoInteractions(repository, stream);
        verify(hash, never()).put(anyString(), anyString(), anyString());
    }

    @Test
    void nullAndEmptyMessagesAreSwallowed() {
        assertThatCode(() -> {
            consumer.consume(null);
            consumer.consume("");
        }).doesNotThrowAnyException();
        verifyNoInteractions(valkey, repository, stream);
    }

    @Test
    void idleThrottleEntriesAreForgotten() {
        consumer.consume(message("AIC860"));
        clock.advance(Duration.ofHours(2));
        assertThat(consumer.forgetIdleCallsigns(Duration.ofHours(1).toMillis())).isEqualTo(1);
    }
}
