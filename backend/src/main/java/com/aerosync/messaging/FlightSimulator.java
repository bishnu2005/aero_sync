package com.aerosync.messaging;

import com.aerosync.model.FlightTelemetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.time.Duration;

 @Component
public class FlightSimulator implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(FlightSimulator.class);
    private final TelemetryProducer producer;

    public FlightSimulator(TelemetryProducer producer) {
        this.producer = producer;
    }

    @Override
    public void run(String... args) {
        log.info("✈️ Starting continuous moving flight simulation...");

        Flux.interval(Duration.ofMillis(500))
                .map(i -> {
                    long flightId = i % 5;
                    double progress = i * 0.002;

                    double lat = 12.97 + (flightId * 0.4) + progress;
                    double lon = 77.59 - (flightId * 0.2) + progress;

                    return new FlightTelemetry(
                            "AIC86" + flightId,
                            "HEX12" + flightId,
                            lat, lon, 30000.0, 450.0,
                            System.currentTimeMillis()
                    );
                })
                .flatMap(producer::publish)
                .subscribe();
    }
}