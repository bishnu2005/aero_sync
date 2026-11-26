package com.aerosync.repository;

import com.aerosync.model.FlightRecord;
import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * All queries bind user input as R2DBC parameters ({@code :name}); nothing is concatenated into SQL.
 */
@Repository
public interface FlightRecordRepository extends ReactiveCrudRepository<FlightRecord, Long> {

    @Query("SELECT * FROM flight_telemetry_history WHERE callsign = :callsign ORDER BY recorded_at DESC LIMIT 200")
    Flux<FlightRecord> findRecentTrailByCallsign(String callsign);

    /** Most recent archived position whose callsign or icao24 matches, ignoring case. */
    @Query("""
            SELECT * FROM flight_telemetry_history
            WHERE UPPER(callsign) = UPPER(:identifier) OR LOWER(icao24) = LOWER(:identifier)
            ORDER BY recorded_at DESC
            LIMIT 1
            """)
    Mono<FlightRecord> findLatestByIdentifier(String identifier);

    /** Latest position per callsign recorded after {@code since} (epoch millis). Used when Valkey is down. */
    @Query("""
            SELECT DISTINCT ON (callsign) * FROM flight_telemetry_history
            WHERE recorded_at >= :since
            ORDER BY callsign, recorded_at DESC
            """)
    Flux<FlightRecord> findLatestSince(long since);

    @Modifying
    @Query("DELETE FROM flight_telemetry_history WHERE recorded_at < :cutoff")
    Mono<Long> deleteOlderThan(long cutoff);
}
