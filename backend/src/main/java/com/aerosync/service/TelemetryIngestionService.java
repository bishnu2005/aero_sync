package com.aerosync.service;

import com.aerosync.exception.OpenSkyRateLimitException;
import com.aerosync.messaging.TelemetryProducer;
import com.aerosync.model.FlightTelemetry;
import com.aerosync.observability.TelemetryMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

/**
 * Polls OpenSky on a fixed delay and publishes every aircraft to Kafka. Every poll first asks
 * {@link TelemetryModeManager} for budget; in simulation mode the API is never touched.
 */
@Service
public class TelemetryIngestionService {

    private static final Logger log = LoggerFactory.getLogger(TelemetryIngestionService.class);
    private static final Duration POLL_DEADLINE = Duration.ofSeconds(45);

    private final AviationApiClient aviationApiClient;
    private final TelemetryProducer telemetryProducer;
    private final TelemetryModeManager telemetryModeManager;
    private final TelemetryMetrics metrics;

    public TelemetryIngestionService(AviationApiClient aviationApiClient,
                                     TelemetryProducer telemetryProducer,
                                     TelemetryModeManager telemetryModeManager,
                                     TelemetryMetrics metrics) {
        this.aviationApiClient = aviationApiClient;
        this.telemetryProducer = telemetryProducer;
        this.telemetryModeManager = telemetryModeManager;
        this.metrics = metrics;
    }

    /**
     * Reactive {@code @Scheduled} (Spring 6.1+): Spring subscribes to the returned Mono and, with fixed delay,
     * waits for it to complete before scheduling the next poll. No thread is parked in {@code block()}.
     * {@code Mono.defer} makes every tick re-check the quota.
     */
    @Scheduled(fixedDelayString = "${aerosync.opensky.poll-interval-ms:60000}",
            initialDelayString = "${aerosync.opensky.initial-delay-ms:5000}")
    public Mono<Void> ingestFlights() {
        return Mono.defer(() -> {
            if (!telemetryModeManager.tryAcquireLiveRequest()) {
                return Mono.empty();
            }
            return pollOnce()
                    .timeout(POLL_DEADLINE)
                    .doOnNext(published -> log.info("Pushed {} live OpenSky flights to Kafka.", published))
                    .onErrorResume(e -> {
                        log.error("OpenSky ingestion cycle failed: {}", e.getMessage());
                        return Mono.empty();
                    })
                    .then();
        });
    }

    /** One OpenSky request; emits how many flights were handed to Kafka. Caller must hold quota. */
    Mono<Long> pollOnce() {
        return aviationApiClient.getLiveFlights()
                .collectList()
                .doOnNext(flights -> {
                    metrics.upstreamSuccess();
                    telemetryModeManager.recordUpstreamSuccess();
                })
                .onErrorResume(this::handleUpstreamFailure)
                .flatMapMany(Flux::fromIterable)
                .concatMap(flight -> telemetryProducer.publish(flight).thenReturn(flight))
                .count()
                .doOnNext(count -> metrics.published(TelemetryMetrics.Source.LIVE, count));
    }

    private Mono<List<FlightTelemetry>> handleUpstreamFailure(Throwable error) {
        if (error instanceof OpenSkyRateLimitException rateLimit) {
            metrics.upstreamRateLimited();
            telemetryModeManager.recordUpstreamRateLimit(rateLimit.getRetryAfter());
        } else {
            metrics.upstreamError();
            log.warn("OpenSky poll failed: {}", error.getMessage());
            telemetryModeManager.recordUpstreamError();
        }
        return Mono.just(List.of());
    }
}
