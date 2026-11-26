package com.aerosync.service;

import com.aerosync.messaging.TelemetryConsumer;
import com.aerosync.repository.FlightRecordRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Clock;
import java.time.Duration;

/**
 * Keeps free-tier stores small: prunes old PostgreSQL history and evicts stale Valkey flights.
 * Both jobs are reactive {@code @Scheduled} methods, so no scheduler thread waits on I/O.
 */
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
    public Mono<Void> pruneHistory() {
        return Mono.defer(() -> repository.deleteOlderThan(clock.millis() - retentionMillis))
                .timeout(DEADLINE)
                .doOnNext(deleted -> log.info("Pruned {} flight history rows older than {} h.", deleted, retentionMillis / 3_600_000))
                .doFinally(signal -> telemetryConsumer.forgetIdleCallsigns(Duration.ofHours(1).toMillis()))
                .onErrorResume(e -> {
                    log.error("Flight history pruning failed: {}", e.getMessage());
                    return Mono.empty();
                })
                .then();
    }

    @Scheduled(fixedDelayString = "${aerosync.cache.eviction-interval-ms:60000}",
            initialDelayString = "${aerosync.cache.eviction-initial-delay-ms:60000}")
    public Mono<Void> evictStaleFlights() {
        return Mono.defer(activeFlightService::evictStale)
                .timeout(DEADLINE)
                .doOnNext(evicted -> {
                    if (evicted > 0) {
                        log.info("Evicted {} stale or corrupted entries from Valkey.", evicted);
                    }
                })
                .onErrorResume(e -> {
                    log.error("Valkey stale-flight eviction failed: {}", e.getMessage());
                    return Mono.empty();
                })
                .then();
    }
}
