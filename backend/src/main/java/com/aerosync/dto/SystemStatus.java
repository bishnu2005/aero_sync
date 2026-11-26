package com.aerosync.dto;

import com.aerosync.service.TelemetryModeManager.FailoverReason;
import com.aerosync.service.TelemetryModeManager.Mode;

import java.time.Instant;

/**
 * Immutable snapshot of the ingestion mode and OpenSky quota, served at {@code GET /api/system/status}. */
public record SystemStatus(
        Mode mode,
        FailoverReason reason,
        String message,
        int requestsUsedToday,
        int dailyLimit,
        Instant windowResetAt,
        long activeFlightCount,
        String feed
) {
    /** Real OpenSky data. */
    public static final String LIVE_FEED = "LIVE_FEED";
    /** Simulator took over automatically (quota, 429 or upstream outage). */
    public static final String SIMULATED_FALLBACK = "SIMULATED_FALLBACK";
    /** Simulator chosen by configuration (OPENSKY_ENABLED=false). */
    public static final String SIMULATED_MANUAL = "SIMULATED_MANUAL";
}
