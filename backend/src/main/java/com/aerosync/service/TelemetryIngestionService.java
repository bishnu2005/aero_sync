package com.aerosync.service;

import com.aerosync.messaging.TelemetryProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class TelemetryIngestionService {

    private static final Logger log = LoggerFactory.getLogger(TelemetryIngestionService.class);

    private final AviationApiClient aviationApiClient;
    private final TelemetryProducer telemetryProducer;

    public TelemetryIngestionService(AviationApiClient aviationApiClient, TelemetryProducer telemetryProducer) {
        this.aviationApiClient = aviationApiClient;
        this.telemetryProducer = telemetryProducer;
    }

    // Slowed down to 30 seconds to prevent OpenSky from rate-limiting your IP
    //@Scheduled(fixedRate = 30000)
    public void ingestFlights() {
        log.info("Fetching real-time flights over India from OpenSky API...");

        aviationApiClient.getLiveFlights()
                .flatMap(telemetryProducer::publish)
                .doOnComplete(() -> log.info("Successfully pushed flight batch to Kafka."))
                .doOnError(error -> log.error("Ingestion error: {}", error.getMessage()))
                .subscribe();
    }
}