package com.aerosync.model;

/**
 * A single aircraft position report as it travels through Kafka, Valkey and the WebSocket stream.
 *
 * @param callsign  flight callsign (e.g. {@code AIC860}), trimmed
 * @param icao24    6-char hex transponder address
 * @param latitude  WGS-84 degrees
 * @param longitude WGS-84 degrees
 * @param altitude  barometric altitude in feet
 * @param velocity  ground speed in metres per second
 * @param heading   true track in degrees clockwise from north (0-360)
 * @param timestamp epoch milliseconds of the position report
 */
public record FlightTelemetry(
        String callsign,
        String icao24,
        double latitude,
        double longitude,
        double altitude,
        double velocity,
        double heading,
        long timestamp
) {
    public FlightTelemetry withCallsign(String newCallsign) {
        return new FlightTelemetry(newCallsign, icao24, latitude, longitude, altitude, velocity, heading, timestamp);
    }

    public static FlightTelemetry fromRecord(FlightRecord r) {
        return new FlightTelemetry(
                r.callsign(),
                r.icao24(),
                r.latitude() != null ? r.latitude() : 0.0,
                r.longitude() != null ? r.longitude() : 0.0,
                r.altitude() != null ? r.altitude() : 0.0,
                r.velocity() != null ? r.velocity() : 0.0,
                r.heading() != null ? r.heading() : 0.0,
                r.recordedAt() != null ? r.recordedAt() : 0L
        );
    }
}
