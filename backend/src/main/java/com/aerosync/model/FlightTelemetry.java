package com.aerosync.model;

public record FlightTelemetry(
        String callsign,
        String icao24,
        double latitude,
        double longitude,
        double altitude,
        double velocity,
        long timestamp
) {}