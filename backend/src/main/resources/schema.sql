CREATE TABLE IF NOT EXISTS flight_telemetry_history (
                                                        id BIGSERIAL PRIMARY KEY,
                                                        callsign VARCHAR(32) NOT NULL,
    icao24 VARCHAR(24) NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    altitude DOUBLE PRECISION,
    velocity DOUBLE PRECISION,
    recorded_at BIGINT NOT NULL
    );

CREATE INDEX IF NOT EXISTS idx_telemetry_callsign_time
    ON flight_telemetry_history (callsign, recorded_at DESC);