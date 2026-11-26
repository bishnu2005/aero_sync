package com.aerosync.dto;

import com.aerosync.service.TelemetryModeManager.FailoverReason;
import com.aerosync.service.TelemetryModeManager.Mode;

import java.time.Instant;

/** Immutable snapshot of the ingestion mode and OpenSky quota, served at {@code GET /api/system/status}. */
public record SystemStatus(
        Mode mode,
        FailoverReason reason,
        String message,
        int requestsUsedToday,
        int dailyLimit,
        Instant windowResetAt,
        long activeFlightCount
) {}
