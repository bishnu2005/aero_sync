-- AeroSync schema. Runs on every boot (spring.sql.init.mode=always) and is fully idempotent,
-- so a fresh free-tier PostgreSQL database (Aiven / Neon / local Docker) initialises itself.

CREATE TABLE IF NOT EXISTS flight_telemetry_history (
    id          BIGSERIAL PRIMARY KEY,
    callsign    VARCHAR(32)      NOT NULL,
    icao24      VARCHAR(24)      NOT NULL,
    latitude    DOUBLE PRECISION NOT NULL,
    longitude   DOUBLE PRECISION NOT NULL,
    altitude    DOUBLE PRECISION,
    velocity    DOUBLE PRECISION,
    heading     DOUBLE PRECISION,
    recorded_at BIGINT           NOT NULL
);

-- Upgrades databases created by earlier AeroSync versions (before heading was tracked).
ALTER TABLE flight_telemetry_history ADD COLUMN IF NOT EXISTS heading DOUBLE PRECISION;

CREATE INDEX IF NOT EXISTS idx_telemetry_callsign_time
    ON flight_telemetry_history (callsign, recorded_at DESC);

-- Case-insensitive Flight ID / Callsign lookups.
CREATE INDEX IF NOT EXISTS idx_telemetry_callsign_upper_time
    ON flight_telemetry_history (UPPER(callsign), recorded_at DESC);

CREATE INDEX IF NOT EXISTS idx_telemetry_icao24_lower_time
    ON flight_telemetry_history (LOWER(icao24), recorded_at DESC);

-- Retention pruning and "recent flights" fallback.
CREATE INDEX IF NOT EXISTS idx_telemetry_recorded_at
    ON flight_telemetry_history (recorded_at);
