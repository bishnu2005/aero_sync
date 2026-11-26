package com.aerosync.service;

import com.aerosync.exception.OpenSkyRateLimitException;
import com.aerosync.messaging.TelemetryProducer;
import com.aerosync.model.FlightTelemetry;
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

    public TelemetryIngestionService(AviationApiClient aviationApiClient,
                                     TelemetryProducer telemetryProducer,
                                     TelemetryModeManager telemetryModeManager) {
        this.aviationApiClient = aviationApiClient;
        this.telemetryProducer = telemetryProducer;
        this.telemetryModeManager = telemetryModeManager;
    }

    @Scheduled(fixedDelayString = "${aerosync.opensky.poll-interval-ms:60000}",
            initialDelayString = "${aerosync.opensky.initial-delay-ms:5000}")
    public void ingestFlights() {
        if (!telemetryModeManager.tryAcquireLiveRequest()) {
            return;
        }
        try {
            Long published = pollOnce().block(POLL_DEADLINE);
            log.info("Pushed {} live OpenSky flights to Kafka.", published);
        } catch (RuntimeException e) {
            log.error("OpenSky ingestion cycle failed: {}", e.getMessage());
        }
    }

    /** One OpenSky request; emits how many flights were handed to Kafka. Caller must hold quota. */
    Mono<Long> pollOnce() {
        return aviationApiClient.getLiveFlights()
                .collectList()
                .doOnNext(flights -> telemetryModeManager.recordUpstreamSuccess())
                .onErrorResume(this::handleUpstreamFailure)
                .flatMapMany(Flux::fromIterable)
                .concatMap(flight -> telemetryProducer.publish(flight).thenReturn(flight))
                .count();
    }

    private Mono<List<FlightTelemetry>> handleUpstreamFailure(Throwable error) {
        if (error instanceof OpenSkyRateLimitException rateLimit) {
            telemetryModeManager.recordUpstreamRateLimit(rateLimit.getRetryAfter());
        } else {
            log.warn("OpenSky poll failed: {}", error.getMessage());
            telemetryModeManager.recordUpstreamError();
        }
        return Mono.just(List.of());
    }
}
