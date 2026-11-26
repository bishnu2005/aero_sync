package com.aerosync.service;

import com.aerosync.dto.SystemStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;

/**
 * Decides whether telemetry comes from OpenSky ({@link Mode#LIVE}) or the {@code FlightSimulator}
 * ({@link Mode#SIMULATION}) and keeps the daily OpenSky request budget.
 *
 * <p>The quota window is the UTC day (OpenSky resets anonymous credits at 00:00 UTC). All state
 * transitions happen under one monitor, so concurrent callers can never over-spend the quota.
 * Time comes from the injected {@link Clock}, so tests can jump past midnight without sleeping.
 *
 * <p>Besides the spec'd transitions, a 429 or "upstream unavailable" failover is retried after a
 * cool-down ({@code aerosync.opensky.failover-cooldown-ms}, default 15 min) instead of waiting for
 * the next UTC day, so a transient OpenSky outage does not cost a whole day of live data.
 */
@Component
public class TelemetryModeManager {

    public enum Mode { LIVE, SIMULATION }

    public enum FailoverReason {
        NONE,
        DAILY_QUOTA_EXHAUSTED,
        UPSTREAM_RATE_LIMITED_429,
        UPSTREAM_UNAVAILABLE,
        MANUAL_SIMULATION_CONFIG
    }

    /** OpenSky's free registered-account ceiling. Anonymous usage is 400/day. */
    public static final int MAX_DAILY_LIMIT = 4000;

    private static final Logger log = LoggerFactory.getLogger(TelemetryModeManager.class);

    private final Clock clock;
    private final boolean enabled;
    private final int dailyLimit;
    private final int consecutiveErrorThreshold;
    private final Duration failoverCooldown;

    // Guarded by "this".
    private Mode mode;
    private FailoverReason reason;
    private int requestsUsedToday;
    private int consecutiveErrors;
    private Instant windowResetAt;
    private Instant resumeLiveAt;

    public TelemetryModeManager(
            @Value("${aerosync.opensky.enabled:true}") boolean enabled,
            @Value("${aerosync.opensky.daily-limit:400}") int dailyLimit,
            @Value("${aerosync.opensky.consecutive-error-threshold:3}") int consecutiveErrorThreshold,
            @Value("${aerosync.opensky.failover-cooldown-ms:900000}") long failoverCooldownMs,
            Clock clock) {
        this.clock = clock;
        this.enabled = enabled;
        this.dailyLimit = clampLimit(dailyLimit);
        this.consecutiveErrorThreshold = Math.max(1, consecutiveErrorThreshold);
        this.failoverCooldown = Duration.ofMillis(Math.max(0, failoverCooldownMs));
        this.windowResetAt = nextUtcMidnight(clock.instant());
        if (enabled) {
            this.mode = Mode.LIVE;
            this.reason = FailoverReason.NONE;
            log.info("OpenSky live ingestion enabled with a daily budget of {} requests.", this.dailyLimit);
        } else {
            this.mode = Mode.SIMULATION;
            this.reason = FailoverReason.MANUAL_SIMULATION_CONFIG;
            log.info("OpenSky ingestion disabled (OPENSKY_ENABLED=false). Flight simulator active.");
        }
    }

    private static int clampLimit(int requested) {
        if (requested < 1) {
            log.warn("OPENSKY_DAILY_LIMIT={} is invalid; using 1.", requested);
            return 1;
        }
        if (requested > MAX_DAILY_LIMIT) {
            log.warn("OPENSKY_DAILY_LIMIT={} exceeds OpenSky's free-tier ceiling; capping at {}.", requested, MAX_DAILY_LIMIT);
            return MAX_DAILY_LIMIT;
        }
        return requested;
    }

    private static Instant nextUtcMidnight(Instant now) {
        return LocalDate.ofInstant(now, ZoneOffset.UTC).plusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant();
    }

    /**
     * Reserves one OpenSky request from today's budget.
     *
     * @return {@code true} if the caller may call OpenSky now; {@code false} if simulation mode is active
     */
    public synchronized boolean tryAcquireLiveRequest() {
        Instant now = clock.instant();
        rollWindowIfExpired(now);

        if (!enabled) {
            return false;
        }
        if (mode == Mode.SIMULATION && resumeLiveAt != null && !now.isBefore(resumeLiveAt)) {
            log.info("OpenSky failover cool-down elapsed ({}); probing live API again.", reason);
            // One more failure re-trips the breaker straight away.
            consecutiveErrors = reason == FailoverReason.UPSTREAM_UNAVAILABLE ? consecutiveErrorThreshold - 1 : 0;
            mode = Mode.LIVE;
            reason = FailoverReason.NONE;
            resumeLiveAt = null;
        }
        if (mode == Mode.SIMULATION) {
            return false;
        }
        if (requestsUsedToday >= dailyLimit) {
            switchToSimulation(FailoverReason.DAILY_QUOTA_EXHAUSTED, null);
            log.warn("Free-tier daily OpenSky quota reached ({}/{}). Switching to flight simulator until {}.",
                    requestsUsedToday, dailyLimit, windowResetAt);
            return false;
        }
        requestsUsedToday++;
        return true;
    }

    private void rollWindowIfExpired(Instant now) {
        if (now.isBefore(windowResetAt)) {
            return;
        }
        requestsUsedToday = 0;
        consecutiveErrors = 0;
        resumeLiveAt = null;
        windowResetAt = nextUtcMidnight(now);
        if (enabled && mode == Mode.SIMULATION) {
            log.info("OpenSky quota window reset; returning to live mode.");
            mode = Mode.LIVE;
            reason = FailoverReason.NONE;
        }
    }

    /** OpenSky answered HTTP 429: fail over immediately. */
    public void recordUpstreamRateLimit() {
        recordUpstreamRateLimit(null);
    }

    /** OpenSky answered HTTP 429: fail over immediately, honouring the server's retry-after hint if longer. */
    public synchronized void recordUpstreamRateLimit(Duration retryAfter) {
        if (!enabled) {
            return;
        }
        Duration wait = retryAfter != null && retryAfter.compareTo(failoverCooldown) > 0 ? retryAfter : failoverCooldown;
        switchToSimulation(FailoverReason.UPSTREAM_RATE_LIMITED_429, clock.instant().plus(wait));
        log.warn("OpenSky returned HTTP 429. Flight simulator active; next live attempt after {}.", resumeLiveAt);
    }

    /** A poll failed (timeout, 4xx/5xx, bad payload). Trips simulation after the configured streak. */
    public synchronized void recordUpstreamError() {
        if (!enabled) {
            return;
        }
        consecutiveErrors++;
        if (mode == Mode.LIVE && consecutiveErrors >= consecutiveErrorThreshold) {
            switchToSimulation(FailoverReason.UPSTREAM_UNAVAILABLE, clock.instant().plus(failoverCooldown));
            log.warn("OpenSky failed {} times in a row. Flight simulator active; next live attempt after {}.",
                    consecutiveErrors, resumeLiveAt);
        }
    }

    public synchronized void recordUpstreamSuccess() {
        consecutiveErrors = 0;
    }

    private void switchToSimulation(FailoverReason why, Instant resumeAt) {
        mode = Mode.SIMULATION;
        reason = why;
        resumeLiveAt = resumeAt;
    }

    public synchronized boolean isSimulationMode() {
        return mode == Mode.SIMULATION;
    }

    public synchronized Mode getMode() {
        return mode;
    }

    public synchronized FailoverReason getReason() {
        return reason;
    }

    public synchronized int getConsecutiveErrors() {
        return consecutiveErrors;
    }

    public int getDailyLimit() {
        return dailyLimit;
    }

    public synchronized SystemStatus getStatus(long activeFlightCount) {
        rollWindowIfExpired(clock.instant());
        return new SystemStatus(mode, reason, describe(), requestsUsedToday, dailyLimit, windowResetAt,
                Math.max(0, activeFlightCount));
    }

    private String describe() {
        return switch (reason) {
            case NONE -> "Live OpenSky radar active (%d/%d polls used today).".formatted(requestsUsedToday, dailyLimit);
            case DAILY_QUOTA_EXHAUSTED -> "Free-tier daily OpenSky quota reached (%d/%d). Live flight simulator active."
                    .formatted(requestsUsedToday, dailyLimit);
            case UPSTREAM_RATE_LIMITED_429 -> "OpenSky rate-limited AeroSync (HTTP 429). Live flight simulator active.";
            case UPSTREAM_UNAVAILABLE -> "OpenSky is unreachable (%d consecutive failures). Live flight simulator active."
                    .formatted(consecutiveErrors);
            case MANUAL_SIMULATION_CONFIG -> "OpenSky ingestion disabled by configuration (OPENSKY_ENABLED=false). Flight simulator active.";
        };
    }
}
