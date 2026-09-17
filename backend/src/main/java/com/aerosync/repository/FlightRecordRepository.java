package com.aerosync.repository;

import com.aerosync.model.FlightRecord;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

@Repository
public interface FlightRecordRepository extends ReactiveCrudRepository<FlightRecord, Long> {

    @Query("SELECT * FROM flight_telemetry_history WHERE callsign = :callsign ORDER BY recorded_at DESC LIMIT 200")
    Flux<FlightRecord> findRecentTrailByCallsign(String callsign);

    @Query("SELECT DISTINCT ON (callsign) * FROM flight_telemetry_history ORDER BY callsign, recorded_at DESC")
    Flux<FlightRecord> findLatestActiveFlights();
}