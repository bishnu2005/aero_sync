package com.aerosync.messaging;

import com.aerosync.model.FlightRecord;
import com.aerosync.model.FlightTelemetry;
import com.aerosync.repository.FlightRecordRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class TelemetryConsumer {

    private static final Logger log = LoggerFactory.getLogger(TelemetryConsumer.class);

    private final TelemetryStream telemetryStream;
    private final FlightRecordRepository repository;
    private final ObjectMapper objectMapper;
    private final ReactiveStringRedisTemplate valkeyTemplate;

    public TelemetryConsumer(TelemetryStream telemetryStream,
                             FlightRecordRepository repository,
                             ReactiveStringRedisTemplate valkeyTemplate) {
        this.telemetryStream = telemetryStream;
        this.repository = repository;
        this.objectMapper = new ObjectMapper();
        this.valkeyTemplate = valkeyTemplate;
    }

    @KafkaListener(topics = TelemetryProducer.TOPIC, groupId = "aerosync-group")
    public void consume(String message) {
        try {
            FlightTelemetry telemetry = objectMapper.readValue(message, FlightTelemetry.class);
            String callsign = telemetry.callsign();

            if (callsign == null || callsign.trim().isEmpty()) {
                return;
            }

            valkeyTemplate.opsForHash().put("active_flights", callsign.trim(), message)
                    .doOnError(e -> log.error("Valkey failed to cache flight: {}", e.getMessage()))
                    .subscribe();

            repository.save(FlightRecord.fromTelemetry(telemetry))
                    .doOnError(e -> log.error("Postgres failed to save flight: {}", e.getMessage()))
                    .subscribe();

            telemetryStream.broadcast(message);

        } catch (Exception e) {
            log.error("Failed to process Kafka message: {}", e.getMessage());
        }
    }
}