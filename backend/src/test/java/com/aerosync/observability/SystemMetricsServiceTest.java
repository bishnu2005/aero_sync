package com.aerosync.observability;

import com.aerosync.dto.SystemMetrics;
import com.aerosync.messaging.TelemetryStream;
import com.aerosync.service.TelemetryModeManager;
import com.aerosync.support.MutableClock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class SystemMetricsServiceTest {

    private final MutableClock clock = MutableClock.at("2026-11-27T10:00:00Z");
    private MeterRegistry registry;
    private TelemetryMetrics metrics;
    private TelemetryStream stream;
    private TelemetryModeManager modeManager;
    private SystemMetricsService service;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new TelemetryMetrics(registry);
        stream = new TelemetryStream();
        modeManager = new TelemetryModeManager(true, 400, 3, 900_000, clock);
        service = new SystemMetricsService(metrics, stream, modeManager, clock);
        new ObservabilityConfig().aerosyncGauges(stream, modeManager).bindTo(registry);
    }

    @Test
    void throughputIsComputedFromTheConsumedCounterOverTheSamplingWindow() {
        for (int i = 0; i < 150; i++) {
            metrics.consumed();
        }
        clock.advance(Duration.ofSeconds(5));
        service.sampleThroughput();
        // Divided by the full 60 s window, not by the 5 s since boot (which would claim 30/s).
        assertThat(service.eventsPerSecond()).isEqualTo(2.5);
    }

    @Test
    void oncePerMinuteLiveBurstsAverageOutInsteadOfReadingZero() {
        // Live OpenSky: 150 aircraft arrive in one burst per 60 s poll.
        for (int minute = 0; minute < 3; minute++) {
            for (int i = 0; i < 150; i++) {
                metrics.consumed();
            }
            for (int tick = 0; tick < 12; tick++) {
                clock.advance(Duration.ofSeconds(5));
                service.sampleThroughput();
                assertThat(service.eventsPerSecond()).as("minute %d tick %d", minute, tick).isPositive();
            }
        }
        assertThat(service.eventsPerSecond()).isEqualTo(2.5);
    }

    @Test
    void neverReadsZeroJustBeforeTheNextLiveBurst() {
        // Burst lands between two samples; just before the following burst the window must still contain it.
        clock.advance(Duration.ofSeconds(2));
        for (int i = 0; i < 150; i++) {
            metrics.consumed();
        }
        for (int tick = 0; tick < 12; tick++) {
            clock.advance(Duration.ofSeconds(5));
            service.sampleThroughput();
            assertThat(service.eventsPerSecond()).as("tick %d", tick).isPositive();
        }
    }

    @Test
    void rateDropsToZeroOnceAWholeWindowPassesWithoutEvents() {
        for (int i = 0; i < 100; i++) {
            metrics.consumed();
        }
        for (int tick = 0; tick < 14; tick++) {
            clock.advance(Duration.ofSeconds(5));
            service.sampleThroughput();
        }
        assertThat(service.eventsPerSecond()).isZero();
    }

    @Test
    void snapshotReportsFeedHitRatioUpstreamAndSubscribers() {
        metrics.lookupHit();
        metrics.lookupHit();
        metrics.lookupHit();
        metrics.lookupMiss();
        metrics.upstreamSuccess();
        metrics.upstreamError();
        metrics.upstreamRateLimited();
        Disposable subscriber = stream.getStream().subscribe();
        clock.advance(Duration.ofSeconds(90));

        SystemMetrics snapshot = service.snapshot();

        assertThat(snapshot.feed()).isEqualTo("LIVE_FEED");
        assertThat(snapshot.cacheHitRatio()).isEqualTo(0.75);
        assertThat(snapshot.cacheLookups()).isEqualTo(4);
        assertThat(snapshot.upstreamRequests()).isEqualTo(3);
        assertThat(snapshot.upstreamErrors()).isEqualTo(1);
        assertThat(snapshot.upstreamRateLimited()).isEqualTo(1);
        assertThat(snapshot.websocketSubscribers()).isEqualTo(1);
        assertThat(snapshot.uptimeSeconds()).isEqualTo(90);
        subscriber.dispose();
    }

    @Test
    void hitRatioIsNullBeforeAnyLookupAndFeedReflectsFailover() {
        modeManager.recordUpstreamRateLimit();
        SystemMetrics snapshot = service.snapshot();
        assertThat(snapshot.cacheHitRatio()).isNull();
        assertThat(snapshot.feed()).isEqualTo("SIMULATED_FALLBACK");

        TelemetryModeManager manual = new TelemetryModeManager(false, 400, 3, 900_000, clock);
        assertThat(new SystemMetricsService(metrics, stream, manual, clock).snapshot().feed()).isEqualTo("SIMULATED_MANUAL");
    }

    @Test
    void gaugesAndTaggedCountersAreRegisteredUnderStableNames() {
        Disposable subscriber = stream.getStream().subscribe();
        modeManager.tryAcquireLiveRequest();
        metrics.published(TelemetryMetrics.Source.SIMULATED, 8);

        assertThat(registry.get("aerosync.websocket.subscribers").gauge().value()).isEqualTo(1.0);
        assertThat(registry.get("aerosync.opensky.quota.used").gauge().value()).isEqualTo(1.0);
        assertThat(registry.get("aerosync.opensky.quota.limit").gauge().value()).isEqualTo(400.0);
        assertThat(registry.get("aerosync.mode.simulation").gauge().value()).isZero();
        assertThat(registry.get("aerosync.telemetry.published").tag("source", "simulated").counter().count()).isEqualTo(8.0);
        subscriber.dispose();
    }
}
