package com.aerosync.observability;

import com.aerosync.dto.SystemMetrics;
import com.aerosync.messaging.TelemetryStream;
import com.aerosync.service.TelemetryModeManager;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.util.ArrayDeque;
import java.util.Deque;
import java.time.Duration;
import java.time.Instant;

/** Builds the dashboard snapshot and derives a throughput rate from the consumed-events counter. */
@Service
public class SystemMetricsService {

    private final TelemetryMetrics metrics;
    private final TelemetryStream telemetryStream;
    private final TelemetryModeManager modeManager;
    private final Clock clock;
    private final Instant startedAt;

    /** Live OpenSky data arrives as one burst per minute, so the rate is averaged over a full poll interval. */
    static final long RATE_WINDOW_MILLIS = 60_000;

    private record Sample(long atMillis, long count) {}

    // Guarded by "this": (time, consumed-count) samples covering the last RATE_WINDOW_MILLIS.
    private final Deque<Sample> samples = new ArrayDeque<>();
    private double eventsPerSecond;

    public SystemMetricsService(TelemetryMetrics metrics, TelemetryStream telemetryStream,
                                TelemetryModeManager modeManager, Clock clock) {
        this.metrics = metrics;
        this.telemetryStream = telemetryStream;
        this.modeManager = modeManager;
        this.clock = clock;
        this.startedAt = clock.instant();
        this.samples.addLast(new Sample(clock.millis(), 0));
    }

    /**
     * Samples the consumed counter every 5 s; the rate is events in the last ~60 s divided by at least 60 s.
     * A cheap in-memory read, so it stays a plain (non-reactive) scheduled method.
     */
    @Scheduled(fixedRate = 5000, initialDelay = 5000)
    public synchronized void sampleThroughput() {
        long now = clock.millis();
        samples.addLast(new Sample(now, metrics.consumedTotal()));
        // Drop the oldest sample only while the next one still spans the full window, so the window is always
        // >= 60 s and includes the latest once-per-minute live burst (no 0.0 reading right before the next poll).
        while (samples.size() > 2 && now - secondOldest().atMillis() >= RATE_WINDOW_MILLIS) {
            samples.removeFirst();
        }
        Sample oldest = samples.peekFirst();
        Sample newest = samples.peekLast();
        // Divide by at least the full window so the first minute after boot is not inflated.
        long elapsed = Math.max(newest.atMillis() - oldest.atMillis(), RATE_WINDOW_MILLIS);
        eventsPerSecond = Math.round((newest.count() - oldest.count()) * 10_000.0 / elapsed) / 10.0;
    }

    private Sample secondOldest() {
        var it = samples.iterator();
        it.next();
        return it.next();
    }

    public synchronized double eventsPerSecond() {
        return eventsPerSecond;
    }

    public SystemMetrics snapshot() {
        long hits = metrics.lookupHits();
        long lookups = hits + metrics.lookupMisses() + metrics.lookupErrors();
        Double hitRatio = lookups == 0 ? null : Math.round(hits * 1000.0 / lookups) / 1000.0;
        long upstreamRequests = metrics.upstreamSuccesses() + metrics.upstreamErrors() + metrics.upstreamRateLimits();
        Instant now = clock.instant();
        return new SystemMetrics(
                modeManager.getStatus(0).feed(),
                eventsPerSecond(),
                metrics.consumedTotal(),
                metrics.publishedLive(),
                metrics.publishedSimulated(),
                metrics.droppedTotal(),
                metrics.kafkaPublishFailures(),
                hitRatio,
                lookups,
                metrics.cacheWriteFailures(),
                metrics.dbWrites(),
                metrics.dbWritesThrottled(),
                metrics.dbWriteFailures(),
                upstreamRequests,
                metrics.upstreamErrors(),
                metrics.upstreamRateLimits(),
                telemetryStream.subscriberCount(),
                Duration.between(startedAt, now).toSeconds(),
                now);
    }
}
