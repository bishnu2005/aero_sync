package com.aerosync.support;

import com.aerosync.model.FlightRecord;
import com.aerosync.model.FlightTelemetry;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

public final class TestFlights {

    public static final ObjectMapper MAPPER = JsonMapper.builder().build();

    private TestFlights() {}

    public static FlightTelemetry flight(String callsign, String icao24, long timestamp) {
        return new FlightTelemetry(callsign, icao24, 28.55, 77.10, 35000, 245, 210, timestamp);
    }

    public static String json(FlightTelemetry flight) {
        return MAPPER.writeValueAsString(flight);
    }

    public static FlightRecord record(String callsign, String icao24, long recordedAt) {
        return new FlightRecord(1L, callsign, icao24, 19.09, 72.87, 12000.0, 180.0, 90.0, recordedAt);
    }
}
