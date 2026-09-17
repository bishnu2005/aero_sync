package com.aerosync.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

@Table("flight_telemetry_history")
public record FlightRecord(
        @Id Long id,
        String callsign,
        String icao24,
        Double latitude,
        Double longitude,
        Double altitude,
        Double velocity,
        Long recordedAt
) {
    public static FlightRecord fromTelemetry(FlightTelemetry t) {
        return new FlightRecord(
                null,
                t.callsign(),
                t.icao24(),
                t.latitude(),
                t.longitude(),
                t.altitude(),
                t.velocity(),
                t.timestamp()
        );
    }
}