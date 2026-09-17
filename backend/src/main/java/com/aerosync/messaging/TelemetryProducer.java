package com.aerosync.messaging;

import com.aerosync.model.FlightTelemetry;
import tools.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
public class TelemetryProducer {

    private static final Logger log = LoggerFactory.getLogger(TelemetryProducer.class);
    public static final String TOPIC = "telemetry.flights.live";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public TelemetryProducer(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public Mono<Void> publish(FlightTelemetry telemetry) {
        try {
            String payload = objectMapper.writeValueAsString(telemetry);
            return Mono.fromFuture(kafkaTemplate.send(TOPIC, telemetry.callsign(), payload))
                    .doOnSuccess(r -> log.info("Published telemetry for {}", telemetry.callsign()))
                    .then();
        } catch (Exception e) {
            log.error("Failed to serialize telemetry", e);
            return Mono.empty();
        }
    }
}