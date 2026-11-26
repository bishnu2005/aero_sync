package com.aerosync.dto;

import com.aerosync.model.FlightTelemetry;

/** Result of a Flight ID / Callsign lookup. */
public record FlightDetailResponse(
        String callsign,
        String icao24,
        double latitude,
        double longitude,
        double altitude,
        double velocity,
        double velocityKnots,
        double heading,
        long timestamp,
        String status,
        String source
) {
    public static final String STATUS_ACTIVE = "ACTIVE";
    public static final String STATUS_HISTORICAL = "HISTORICAL";
    public static final String SOURCE_VALKEY = "VALKEY_CACHE";
    public static final String SOURCE_POSTGRES = "POSTGRES_ARCHIVE";

    private static final double MPS_TO_KNOTS = 1.943844;

    public static FlightDetailResponse of(FlightTelemetry t, String status, String source) {
        double knots = Math.round(t.velocity() * MPS_TO_KNOTS * 10.0) / 10.0;
        return new FlightDetailResponse(
                t.callsign(), t.icao24(), t.latitude(), t.longitude(), t.altitude(),
                t.velocity(), knots, t.heading(), t.timestamp(), status, source);
    }
}
