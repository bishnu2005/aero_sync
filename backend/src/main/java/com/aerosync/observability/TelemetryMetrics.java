package com.aerosync.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/**
 * Custom Micrometer meters for the telemetry pipeline. Every counter is exported at
 * {@code /actuator/metrics/<name>} and {@code /actuator/prometheus}, and summarised for the dashboard
 * by {@link SystemMetricsService}.
 *
 * <pre>
 * aerosync.telemetry.published{source=live|simulated}   events handed to Kafka by each producer side
 * aerosync.kafka.publish{result=success|failure}         Kafka send outcomes
 * aerosync.telemetry.consumed                            Kafka records accepted by the consumer
 * aerosync.telemetry.dropped{reason=malformed|blank_callsign}
 * aerosync.cache.writes{result=success|failure}          Valkey HSET outcomes
 * aerosync.db.writes{result=success|failure|throttled}   PostgreSQL archive writes
 * aerosync.cache.lookups{result=hit|miss|error}          Flight lookups served by Valkey vs. fallback
 * aerosync.opensky.requests{outcome=success|rate_limited|error}
 * </pre>
 */
@Component
public class TelemetryMetrics {

    public enum Source { LIVE, SIMULATED }

    private final Counter publishedLive;
    private final Counter publishedSimulated;
    private final Counter kafkaPublishSuccess;
    private final Counter kafkaPublishFailure;
    private final Counter consumed;
    private final Counter droppedMalformed;
    private final Counter droppedBlankCallsign;
    private final Counter cacheWriteSuccess;
    private final Counter cacheWriteFailure;
    private final Counter dbWriteSuccess;
    private final Counter dbWriteFailure;
    private final Counter dbWriteThrottled;
    private final Counter lookupHit;
    private final Counter lookupMiss;
    private final Counter lookupError;
    private final Counter upstreamSuccess;
    private final Counter upstreamRateLimited;
    private final Counter upstreamError;

    public TelemetryMetrics(MeterRegistry registry) {
        publishedLive = counter(registry, "aerosync.telemetry.published", "Telemetry events published to Kafka", "source", "live");
        publishedSimulated = counter(registry, "aerosync.telemetry.published", "Telemetry events published to Kafka", "source", "simulated");
        kafkaPublishSuccess = counter(registry, "aerosync.kafka.publish", "Kafka send outcomes", "result", "success");
        kafkaPublishFailure = counter(registry, "aerosync.kafka.publish", "Kafka send outcomes", "result", "failure");
        consumed = Counter.builder("aerosync.telemetry.consumed").description("Kafka telemetry records accepted").register(registry);
        droppedMalformed = counter(registry, "aerosync.telemetry.dropped", "Kafka telemetry records dropped", "reason", "malformed");
        droppedBlankCallsign = counter(registry, "aerosync.telemetry.dropped", "Kafka telemetry records dropped", "reason", "blank_callsign");
        cacheWriteSuccess = counter(registry, "aerosync.cache.writes", "Valkey active-flight writes", "result", "success");
        cacheWriteFailure = counter(registry, "aerosync.cache.writes", "Valkey active-flight writes", "result", "failure");
        dbWriteSuccess = counter(registry, "aerosync.db.writes", "PostgreSQL history writes", "result", "success");
        dbWriteFailure = counter(registry, "aerosync.db.writes", "PostgreSQL history writes", "result", "failure");
        dbWriteThrottled = counter(registry, "aerosync.db.writes", "PostgreSQL history writes", "result", "throttled");
        lookupHit = counter(registry, "aerosync.cache.lookups", "Flight lookups by cache outcome", "result", "hit");
        lookupMiss = counter(registry, "aerosync.cache.lookups", "Flight lookups by cache outcome", "result", "miss");
        lookupError = counter(registry, "aerosync.cache.lookups", "Flight lookups by cache outcome", "result", "error");
        upstreamSuccess = counter(registry, "aerosync.opensky.requests", "OpenSky API requests", "outcome", "success");
        upstreamRateLimited = counter(registry, "aerosync.opensky.requests", "OpenSky API requests", "outcome", "rate_limited");
        upstreamError = counter(registry, "aerosync.opensky.requests", "OpenSky API requests", "outcome", "error");
    }

    private static Counter counter(MeterRegistry registry, String name, String description, String tag, String value) {
        return Counter.builder(name).description(description).tag(tag, value).register(registry);
    }

    public void published(Source source, long count) {
        (source == Source.LIVE ? publishedLive : publishedSimulated).increment(count);
    }

    public void kafkaPublish(boolean success) {
        (success ? kafkaPublishSuccess : kafkaPublishFailure).increment();
    }

    public void consumed() {
        consumed.increment();
    }

    public void droppedMalformed() {
        droppedMalformed.increment();
    }

    public void droppedBlankCallsign() {
        droppedBlankCallsign.increment();
    }

    public void cacheWrite(boolean success) {
        (success ? cacheWriteSuccess : cacheWriteFailure).increment();
    }

    public void dbWriteSucceeded() {
        dbWriteSuccess.increment();
    }

    public void dbWriteFailed() {
        dbWriteFailure.increment();
    }

    public void dbWriteThrottled() {
        dbWriteThrottled.increment();
    }

    public void lookupHit() {
        lookupHit.increment();
    }

    public void lookupMiss() {
        lookupMiss.increment();
    }

    public void lookupError() {
        lookupError.increment();
    }

    public void upstreamSuccess() {
        upstreamSuccess.increment();
    }

    public void upstreamRateLimited() {
        upstreamRateLimited.increment();
    }

    public void upstreamError() {
        upstreamError.increment();
    }

    // ---- snapshot accessors for the dashboard summary ----

    public long publishedLive() { return (long) publishedLive.count(); }
    public long publishedSimulated() { return (long) publishedSimulated.count(); }
    public long kafkaPublishFailures() { return (long) kafkaPublishFailure.count(); }
    public long consumedTotal() { return (long) consumed.count(); }
    public long droppedTotal() { return (long) (droppedMalformed.count() + droppedBlankCallsign.count()); }
    public long cacheWriteFailures() { return (long) cacheWriteFailure.count(); }
    public long dbWrites() { return (long) dbWriteSuccess.count(); }
    public long dbWriteFailures() { return (long) dbWriteFailure.count(); }
    public long dbWritesThrottled() { return (long) dbWriteThrottled.count(); }
    public long lookupHits() { return (long) lookupHit.count(); }
    public long lookupMisses() { return (long) lookupMiss.count(); }
    public long lookupErrors() { return (long) lookupError.count(); }
    public long upstreamSuccesses() { return (long) upstreamSuccess.count(); }
    public long upstreamRateLimits() { return (long) upstreamRateLimited.count(); }
    public long upstreamErrors() { return (long) upstreamError.count(); }
}
