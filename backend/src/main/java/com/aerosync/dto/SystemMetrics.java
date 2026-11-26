package com.aerosync.dto;

import java.time.Instant;

/**
 * Compact pipeline health snapshot for the dashboard status bar ({@code GET /api/system/metrics}).
 * The full meter set lives at {@code /actuator/metrics} and {@code /actuator/prometheus}.
 *
 * @param feed            LIVE_FEED, SIMULATED_FALLBACK or SIMULATED_MANUAL
 * @param eventsPerSecond Kafka records consumed per second over the last sampling window
 * @param cacheHitRatio   share of flight lookups answered by Valkey, or {@code null} before any lookup
 */
public record SystemMetrics(
        String feed,
        double eventsPerSecond,
        long eventsConsumed,
        long eventsPublishedLive,
        long eventsPublishedSimulated,
        long eventsDropped,
        long kafkaPublishFailures,
        Double cacheHitRatio,
        long cacheLookups,
        long cacheWriteFailures,
        long dbWrites,
        long dbWritesThrottled,
        long dbWriteFailures,
        long upstreamRequests,
        long upstreamErrors,
        long upstreamRateLimited,
        int websocketSubscribers,
        long uptimeSeconds,
        Instant timestamp
) {}
