package com.aerosync.messaging;

import com.aerosync.model.FlightTelemetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import com.aerosync.observability.TelemetryMetrics;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import tools.jackson.databind.ObjectMapper;

@Service
public class TelemetryProducer {

    private static final Logger log = LoggerFactory.getLogger(TelemetryProducer.class);
    public static final String TOPIC = "telemetry.flights.live";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final TelemetryMetrics metrics;

    public TelemetryProducer(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper, TelemetryMetrics metrics) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.metrics = metrics;
    }

    /**
     * Publishes one position report keyed by callsign. The send is deferred until subscription and
     * broker failures are logged and swallowed, so one bad send never aborts a batch or a simulator tick.
     *
     * <p>{@code KafkaTemplate.send()} is not fully non-blocking: when topic metadata is missing it blocks the
     * calling thread for up to {@code max.block.ms}. In the live path the caller is the Netty event loop that
     * delivered the OpenSky response, so the send is shifted to {@code boundedElastic} to keep the loop free.
     */
    public Mono<Void> publish(FlightTelemetry telemetry) {
        return Mono.fromCallable(() -> objectMapper.writeValueAsString(telemetry))
                .flatMap(payload -> Mono.fromFuture(() -> kafkaTemplate.send(TOPIC, telemetry.callsign(), payload)))
                .subscribeOn(Schedulers.boundedElastic())
                .doOnSuccess(r -> {
                    metrics.kafkaPublish(true);
                    log.debug("Published telemetry for {}", telemetry.callsign());
                })
                .onErrorResume(e -> {
                    metrics.kafkaPublish(false);
                    log.error("Failed to publish telemetry for {}: {}", telemetry.callsign(), e.getMessage());
                    return Mono.empty();
                })
                .then();
    }
}
