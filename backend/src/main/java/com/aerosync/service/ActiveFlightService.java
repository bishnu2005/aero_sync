package com.aerosync.service;

import com.aerosync.exception.ServiceUnavailableException;
import com.aerosync.model.FlightTelemetry;
import com.aerosync.repository.FlightRecordRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.ReactiveHashOperations;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tools.jackson.databind.ObjectMapper;

import java.time.Clock;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Read side of the Valkey {@value #ACTIVE_FLIGHTS_KEY} hash (field = callsign, value = telemetry JSON).
 * Corrupted values are logged and skipped, and entries older than
 * {@code aerosync.cache.stale-seconds} are treated as gone.
 *
 * <p>Reads are feed-consistent: while the live feed is active, simulator-generated entries are hidden, and while
 * the simulator is active, real OpenSky entries are hidden. So a mode switch never shows synthetic aircraft under
 * a "LIVE FEED" label (or vice versa), and {@link #evictStale()} removes the other feed's leftovers.
 */
@Service
public class ActiveFlightService {

    public static final String ACTIVE_FLIGHTS_KEY = "active_flights";
    private static final Logger log = LoggerFactory.getLogger(ActiveFlightService.class);
    private static final Comparator<FlightTelemetry> BY_CALLSIGN =
            Comparator.comparing(FlightTelemetry::callsign, String.CASE_INSENSITIVE_ORDER)
                    .thenComparing(FlightTelemetry::icao24, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER));

    private final ReactiveStringRedisTemplate valkeyTemplate;
    private final FlightRecordRepository repository;
    private final ObjectMapper objectMapper;
    private final Clock clock;
    private final TelemetryModeManager modeManager;
    private final long staleMillis;

    public ActiveFlightService(ReactiveStringRedisTemplate valkeyTemplate,
                               FlightRecordRepository repository,
                               ObjectMapper objectMapper,
                               Clock clock,
                               TelemetryModeManager modeManager,
                               @Value("${aerosync.cache.stale-seconds:300}") long staleSeconds) {
        this.valkeyTemplate = valkeyTemplate;
        this.repository = repository;
        this.objectMapper = objectMapper;
        this.clock = clock;
        this.modeManager = modeManager;
        this.staleMillis = Math.max(1, staleSeconds) * 1000;
    }

    private ReactiveHashOperations<String, String, String> hash() {
        return valkeyTemplate.opsForHash();
    }

    /**
     * Active, non-stale flights sorted by callsign. Falls back to recent PostgreSQL positions if Valkey
     * is down; emits {@link ServiceUnavailableException} only when both stores fail.
     */
    public Flux<FlightTelemetry> getActiveFlights() {
        return readCache()
                .onErrorResume(cacheError -> {
                    log.warn("Valkey unavailable for active flights ({}); falling back to PostgreSQL.", cacheError.getMessage());
                    return readRecentFromArchive().onErrorMap(dbError -> {
                        log.error("PostgreSQL fallback also failed: {}", dbError.getMessage());
                        return new ServiceUnavailableException("Flight data stores are temporarily unavailable.", dbError);
                    });
                })
                .flatMapIterable(list -> list);
    }

    /** Number of active flights for the status endpoint; {@code 0} if Valkey is unreachable. */
    public Mono<Long> countActiveFlights() {
        return readCache()
                .map(list -> (long) list.size())
                .onErrorResume(e -> {
                    log.warn("Valkey unavailable while counting active flights: {}", e.getMessage());
                    return Mono.just(0L);
                });
    }

    /** Case-insensitive exact match on callsign or icao24 among non-stale cached flights. Errors propagate. */
    public Mono<FlightTelemetry> findInCache(String identifier) {
        String needle = identifier.toUpperCase(Locale.ROOT);
        return readCache()
                .flatMapIterable(list -> list)
                .filter(f -> needle.equals(upper(f.callsign())) || needle.equals(upper(f.icao24())))
                .next();
    }

    /** Removes stale and unparseable entries from the hash. Emits the number of removed fields. */
    public Mono<Long> evictStale() {
        long now = clock.millis();
        return hash().entries(ACTIVE_FLIGHTS_KEY)
                .filter(entry -> {
                    FlightTelemetry flight = parse(entry.getValue());
                    return flight == null || isStale(flight, now) || !matchesCurrentFeed(flight);
                })
                .map(Map.Entry::getKey)
                .collectList()
                .flatMap(keys -> keys.isEmpty()
                        ? Mono.just(0L)
                        : hash().remove(ACTIVE_FLIGHTS_KEY, keys.toArray()));
    }

    private Mono<List<FlightTelemetry>> readCache() {
        long now = clock.millis();
        return hash().values(ACTIVE_FLIGHTS_KEY)
                .<FlightTelemetry>handle((json, sink) -> {
                    FlightTelemetry flight = parse(json);
                    if (flight != null && !isStale(flight, now) && matchesCurrentFeed(flight)) {
                        sink.next(flight);
                    }
                })
                .collectSortedList(BY_CALLSIGN);
    }

    private Mono<List<FlightTelemetry>> readRecentFromArchive() {
        return repository.findLatestSince(clock.millis() - staleMillis)
                .map(FlightTelemetry::fromRecord)
                .filter(f -> f.callsign() != null && !f.callsign().isBlank() && matchesCurrentFeed(f))
                .collectSortedList(BY_CALLSIGN);
    }

    private FlightTelemetry parse(String json) {
        if (json == null) {
            return null;
        }
        try {
            FlightTelemetry flight = objectMapper.readValue(json, FlightTelemetry.class);
            if (flight == null || flight.callsign() == null || flight.callsign().isBlank()) {
                log.warn("Skipping Valkey entry without callsign.");
                return null;
            }
            return flight;
        } catch (RuntimeException e) {
            log.warn("Skipping corrupted Valkey entry: {}", e.getMessage());
            return null;
        }
    }

    /** Simulator events belong to simulation mode; everything else (OpenSky, untagged legacy) to live mode. */
    private boolean matchesCurrentFeed(FlightTelemetry flight) {
        return flight.isSimulated() == modeManager.isSimulationMode();
    }

    private boolean isStale(FlightTelemetry flight, long now) {
        return now - flight.timestamp() > staleMillis;
    }

    private static String upper(String value) {
        return value == null ? null : value.trim().toUpperCase(Locale.ROOT);
    }
}
