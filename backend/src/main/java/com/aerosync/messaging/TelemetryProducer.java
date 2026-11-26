package com.aerosync.messaging;

import com.aerosync.model.FlightTelemetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import tools.jackson.databind.ObjectMapper;

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

    /**
     * Publishes one position report keyed by callsign. The send is deferred until subscription and
     * broker failures are logged and swallowed, so one bad send never aborts a batch or a simulator tick.
     */
    public Mono<Void> publish(FlightTelemetry telemetry) {
        return Mono.fromCallable(() -> objectMapper.writeValueAsString(telemetry))
                .flatMap(payload -> Mono.fromFuture(() -> kafkaTemplate.send(TOPIC, telemetry.callsign(), payload)))
                .doOnSuccess(r -> log.debug("Published telemetry for {}", telemetry.callsign()))
                .onErrorResume(e -> {
                    log.error("Failed to publish telemetry for {}: {}", telemetry.callsign(), e.getMessage());
                    return Mono.empty();
                })
                .then();
    }
}
