package com.aerosync.messaging;

import com.aerosync.model.FlightRecord;
import com.aerosync.model.FlightTelemetry;
import com.aerosync.observability.TelemetryMetrics;
import com.aerosync.repository.FlightRecordRepository;
import com.aerosync.service.ActiveFlightService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Kafka -> (Valkey cache, PostgreSQL archive, WebSocket stream).
 *
 * <p>The three sinks are isolated from each other: a failure in one is logged and never stops the
 * other two, and no exception ever escapes to the Kafka listener container, so a bad record cannot
 * poison the consumer loop. PostgreSQL writes are throttled per callsign
 * ({@code aerosync.db.persist-interval-seconds}) to stay inside free-tier storage and connection limits;
 * Valkey and the WebSocket stream are updated on every message.
 */
@Service
public class TelemetryConsumer {

    private static final Logger log = LoggerFactory.getLogger(TelemetryConsumer.class);

    private final TelemetryStream telemetryStream;
    private final FlightRecordRepository repository;
    private final ObjectMapper objectMapper;
    private final ReactiveStringRedisTemplate valkeyTemplate;
    private final Clock clock;
    private final TelemetryMetrics metrics;
    private final long persistIntervalMillis;
    private final Map<String, Long> lastPersistedAt = new ConcurrentHashMap<>();

    public TelemetryConsumer(TelemetryStream telemetryStream,
                             FlightRecordRepository repository,
                             ReactiveStringRedisTemplate valkeyTemplate,
                             ObjectMapper objectMapper,
                             Clock clock,
                             TelemetryMetrics metrics,
                             @Value("${aerosync.db.persist-interval-seconds:15}") long persistIntervalSeconds) {
        this.telemetryStream = telemetryStream;
        this.repository = repository;
        this.valkeyTemplate = valkeyTemplate;
        this.objectMapper = objectMapper;
        this.clock = clock;
        this.metrics = metrics;
        this.persistIntervalMillis = Math.max(0, persistIntervalSeconds) * 1000;
    }

    @KafkaListener(topics = TelemetryProducer.TOPIC, groupId = "${aerosync.kafka.consumer-group:aerosync-group}")
    public void consume(String message) {
        FlightTelemetry telemetry = parse(message);
        if (telemetry == null) {
            return;
        }
        metrics.consumed();
        String callsign = telemetry.callsign().trim();
        FlightTelemetry normalized = telemetry.withCallsign(callsign);

        String payload;
        try {
            payload = objectMapper.writeValueAsString(normalized);
        } catch (RuntimeException e) {
            log.error("Failed to re-serialise telemetry for {}: {}", callsign, e.getMessage());
            return;
        }

        cache(callsign, payload);
        persistThrottled(callsign, normalized);
        broadcast(payload);
    }

    private FlightTelemetry parse(String message) {
        if (message == null || message.isBlank()) {
            metrics.droppedMalformed();
            return null;
        }
        try {
            FlightTelemetry telemetry = objectMapper.readValue(message, FlightTelemetry.class);
            if (telemetry == null || telemetry.callsign() == null || telemetry.callsign().isBlank()) {
                metrics.droppedBlankCallsign();
                return null;
            }
            return telemetry;
        } catch (RuntimeException e) {
            metrics.droppedMalformed();
            log.warn("Dropping malformed telemetry message: {}", e.getMessage());
            return null;
        }
    }

    private void cache(String callsign, String payload) {
        try {
            valkeyTemplate.<String, String>opsForHash()
                    .put(ActiveFlightService.ACTIVE_FLIGHTS_KEY, callsign, payload)
                    .subscribe(ok -> metrics.cacheWrite(true), e -> {
                        metrics.cacheWrite(false);
                        log.error("Valkey failed to cache {}: {}", callsign, e.getMessage());
                    });
        } catch (RuntimeException e) {
            metrics.cacheWrite(false);
            log.error("Valkey failed to cache {}: {}", callsign, e.getMessage());
        }
    }

    private void persistThrottled(String callsign, FlightTelemetry telemetry) {
        if (!shouldPersist(callsign)) {
            metrics.dbWriteThrottled();
            return;
        }
        try {
            repository.save(FlightRecord.fromTelemetry(telemetry))
                    .subscribe(saved -> metrics.dbWriteSucceeded(), e -> {
                        metrics.dbWriteFailed();
                        lastPersistedAt.remove(callsign); // retry on the next message
                        log.error("PostgreSQL failed to save {}: {}", callsign, e.getMessage());
                    });
        } catch (RuntimeException e) {
            metrics.dbWriteFailed();
            lastPersistedAt.remove(callsign);
            log.error("PostgreSQL failed to save {}: {}", callsign, e.getMessage());
        }
    }

    /** Atomically claims the persist slot for this callsign if the throttle window has elapsed. */
    boolean shouldPersist(String callsign) {
        long now = clock.millis();
        boolean[] claimed = {false};
        lastPersistedAt.compute(callsign, (key, last) -> {
            if (last == null || now - last >= persistIntervalMillis) {
                claimed[0] = true;
                return now;
            }
            return last;
        });
        return claimed[0];
    }

    private void broadcast(String payload) {
        try {
            telemetryStream.broadcast(payload);
        } catch (RuntimeException e) {
            log.error("WebSocket broadcast failed: {}", e.getMessage());
        }
    }

    /** Drops throttle bookkeeping for aircraft not seen for a while, so the map cannot grow forever. */
    public int forgetIdleCallsigns(long idleMillis) {
        long cutoff = clock.millis() - Math.max(idleMillis, persistIntervalMillis);
        int before = lastPersistedAt.size();
        lastPersistedAt.values().removeIf(last -> last < cutoff);
        return before - lastPersistedAt.size();
    }
}
