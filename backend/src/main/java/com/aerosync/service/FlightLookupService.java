package com.aerosync.service;

import com.aerosync.dto.FlightDetailResponse;
import com.aerosync.exception.FlightNotFoundException;
import com.aerosync.exception.ServiceUnavailableException;
import com.aerosync.model.FlightTelemetry;
import com.aerosync.observability.TelemetryMetrics;
import com.aerosync.repository.FlightRecordRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.aerosync.dto.FlightDetailResponse.SOURCE_POSTGRES;
import static com.aerosync.dto.FlightDetailResponse.SOURCE_VALKEY;
import static com.aerosync.dto.FlightDetailResponse.STATUS_ACTIVE;
import static com.aerosync.dto.FlightDetailResponse.STATUS_HISTORICAL;

/** Flight ID / Callsign lookup: Valkey live cache first, PostgreSQL archive second. */
@Service
public class FlightLookupService {

    private static final Logger log = LoggerFactory.getLogger(FlightLookupService.class);

    private final ActiveFlightService activeFlightService;
    private final FlightRecordRepository repository;
    private final TelemetryMetrics metrics;

    public FlightLookupService(ActiveFlightService activeFlightService, FlightRecordRepository repository,
                               TelemetryMetrics metrics) {
        this.activeFlightService = activeFlightService;
        this.repository = repository;
        this.metrics = metrics;
    }

    /** @param identifier an already validated callsign or icao24 */
    public Mono<FlightDetailResponse> lookup(String identifier) {
        AtomicBoolean cacheFailed = new AtomicBoolean(false);
        Mono<FlightDetailResponse> fromCache = activeFlightService.findInCache(identifier)
                .map(t -> FlightDetailResponse.of(t, STATUS_ACTIVE, SOURCE_VALKEY))
                .doOnNext(hit -> metrics.lookupHit())
                .onErrorResume(e -> {
                    cacheFailed.set(true);
                    metrics.lookupError();
                    log.warn("Valkey lookup failed for '{}' ({}); falling back to PostgreSQL.", identifier, e.getMessage());
                    return Mono.empty();
                });

        Mono<FlightDetailResponse> fromArchive = Mono.defer(() -> repository.findLatestByIdentifier(identifier))
                .map(r -> FlightDetailResponse.of(FlightTelemetry.fromRecord(r), STATUS_HISTORICAL, SOURCE_POSTGRES))
                .onErrorMap(e -> {
                    log.error("PostgreSQL lookup failed for '{}': {}", identifier, e.getMessage());
                    return new ServiceUnavailableException("Flight archive is temporarily unavailable.", e);
                });

        return fromCache
                .switchIfEmpty(Mono.defer(() -> {
                    if (!cacheFailed.get()) {
                        metrics.lookupMiss();
                    }
                    return fromArchive;
                }))
                .switchIfEmpty(Mono.error(() -> new FlightNotFoundException(identifier)));
    }
}
