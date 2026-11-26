package com.aerosync.service;

import com.aerosync.messaging.TelemetryConsumer;
import com.aerosync.repository.FlightRecordRepository;
import com.aerosync.support.MutableClock;
import com.aerosync.support.TestFlights;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.ReactiveHashOperations;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.Map;

import static com.aerosync.support.TestFlights.flight;
import static com.aerosync.support.TestFlights.json;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DataRetentionServiceTest {

    private final MutableClock clock = MutableClock.at("2026-11-27T10:00:00Z");
    private FlightRecordRepository repository;
    private ReactiveHashOperations<String, String, String> hash;
    private ActiveFlightService activeFlightService;
    private DataRetentionService service;
    private TelemetryModeManager modeManager;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        repository = mock(FlightRecordRepository.class);
        ReactiveStringRedisTemplate valkey = mock(ReactiveStringRedisTemplate.class);
        hash = mock(ReactiveHashOperations.class);
        when(valkey.<String, String>opsForHash()).thenReturn(hash);
        modeManager = new TelemetryModeManager(true, 400, 3, 900_000, clock);
        activeFlightService = new ActiveFlightService(valkey, repository, TestFlights.MAPPER, clock, modeManager, 300);
        service = new DataRetentionService(repository, activeFlightService, mock(TelemetryConsumer.class), clock, 24);
    }

    private static Map.Entry<String, String> entry(String key, String value) {
        return new AbstractMap.SimpleEntry<>(key, value);
    }

    @Test
    void prunesRowsOlderThanRetentionWindow() {
        when(repository.deleteOlderThan(anyLong())).thenReturn(Mono.just(42L));

        service.pruneHistory().block();

        verify(repository).deleteOlderThan(clock.millis() - Duration.ofHours(24).toMillis());
    }

    @Test
    void pruneFailureIsLoggedNotThrown() {
        when(repository.deleteOlderThan(anyLong())).thenReturn(Mono.error(new IllegalStateException("db down")));
        assertThatCode(() -> service.pruneHistory().block()).doesNotThrowAnyException();
    }

    @Test
    void evictsStaleAndCorruptedEntriesOnly() {
        long now = clock.millis();
        when(hash.entries("active_flights")).thenReturn(Flux.just(
                entry("AIC860", json(flight("AIC860", "800a01", now))),
                entry("OLD001", json(flight("OLD001", "aaaaaa", now - Duration.ofMinutes(6).toMillis()))),
                entry("BROKEN", "{oops")));
        when(hash.remove(eq("active_flights"), any(Object[].class))).thenReturn(Mono.just(2L));

        StepVerifier.create(activeFlightService.evictStale()).expectNext(2L).verifyComplete();

        verify(hash).remove("active_flights", "OLD001", "BROKEN");
    }

    @Test
    void evictsTheOtherFeedsLeftoversAfterAModeSwitch() {
        long now = clock.millis();
        when(hash.entries("active_flights")).thenReturn(Flux.just(
                entry("AIC860", json(TestFlights.simulated("AIC860", "800a01", now))),
                entry("ABY800", json(flight("ABY800", "896558", now)))));
        when(hash.remove(eq("active_flights"), any(Object[].class))).thenReturn(Mono.just(1L));

        activeFlightService.evictStale().block();            // live mode: simulator leftovers go
        verify(hash).remove("active_flights", "AIC860");

        modeManager.recordUpstreamRateLimit();               // now in simulated fallback
        activeFlightService.evictStale().block();            // real OpenSky leftovers go
        verify(hash).remove("active_flights", "ABY800");
    }

    @Test
    void nothingToEvictSkipsTheDelete() {
        when(hash.entries("active_flights")).thenReturn(Flux.just(entry("AIC860", json(flight("AIC860", "800a01", clock.millis())))));

        service.evictStaleFlights().block();

        verify(hash, never()).remove(any(), any(Object[].class));
    }

    @Test
    void evictionFailureIsLoggedNotThrown() {
        when(hash.entries("active_flights")).thenReturn(Flux.error(new IllegalStateException("valkey down")));
        assertThatCode(() -> service.evictStaleFlights().block()).doesNotThrowAnyException();
    }
}
