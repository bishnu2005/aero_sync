package com.aerosync.service;

import com.aerosync.messaging.TelemetryConsumer;
import com.aerosync.repository.FlightRecordRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Duration;

/** Keeps free-tier stores small: prunes old PostgreSQL history and evicts stale Valkey flights. */
@Service
public class DataRetentionService {

    private static final Logger log = LoggerFactory.getLogger(DataRetentionService.class);
    private static final Duration DEADLINE = Duration.ofSeconds(60);

    private final FlightRecordRepository repository;
    private final ActiveFlightService activeFlightService;
    private final TelemetryConsumer telemetryConsumer;
    private final Clock clock;
    private final long retentionMillis;

    public DataRetentionService(FlightRecordRepository repository,
                                ActiveFlightService activeFlightService,
                                TelemetryConsumer telemetryConsumer,
                                Clock clock,
                                @Value("${aerosync.db.retention-hours:24}") long retentionHours) {
        this.repository = repository;
        this.activeFlightService = activeFlightService;
        this.telemetryConsumer = telemetryConsumer;
        this.clock = clock;
        this.retentionMillis = Duration.ofHours(Math.max(1, retentionHours)).toMillis();
    }

    @Scheduled(fixedDelayString = "${aerosync.db.prune-interval-ms:3600000}",
            initialDelayString = "${aerosync.db.prune-initial-delay-ms:60000}")
    public void pruneHistory() {
        try {
            long cutoff = clock.millis() - retentionMillis;
            Long deleted = repository.deleteOlderThan(cutoff).block(DEADLINE);
            log.info("Pruned {} flight history rows older than {} h.", deleted, retentionMillis / 3_600_000);
            telemetryConsumer.forgetIdleCallsigns(Duration.ofHours(1).toMillis());
        } catch (RuntimeException e) {
            log.error("Flight history pruning failed: {}", e.getMessage());
        }
    }

    @Scheduled(fixedDelayString = "${aerosync.cache.eviction-interval-ms:60000}",
            initialDelayString = "${aerosync.cache.eviction-initial-delay-ms:60000}")
    public void evictStaleFlights() {
        try {
            Long evicted = activeFlightService.evictStale().block(DEADLINE);
            if (evicted != null && evicted > 0) {
                log.info("Evicted {} stale or corrupted entries from Valkey.", evicted);
            }
        } catch (RuntimeException e) {
            log.error("Valkey stale-flight eviction failed: {}", e.getMessage());
        }
    }
}
