package com.aerosync.service;

import com.aerosync.dto.SystemStatus;
import com.aerosync.service.TelemetryModeManager.FailoverReason;
import com.aerosync.service.TelemetryModeManager.Mode;
import com.aerosync.support.MutableClock;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;

import java.lang.reflect.Constructor;
import java.lang.reflect.Parameter;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class TelemetryModeManagerTest {

    private static final long COOLDOWN_MS = Duration.ofMinutes(15).toMillis();

    private final MutableClock clock = MutableClock.at("2026-11-27T10:00:00Z");

    private TelemetryModeManager manager(int limit) {
        return new TelemetryModeManager(true, limit, 3, COOLDOWN_MS, clock);
    }

    @Test
    void defaultDailyLimitIsTheFullAnonymousFreeTierOf400() throws Exception {
        Constructor<?> ctor = TelemetryModeManager.class.getConstructors()[0];
        String dailyLimitDefault = null;
        for (Parameter p : ctor.getParameters()) {
            Value v = p.getAnnotation(Value.class);
            if (v != null && v.value().startsWith("${aerosync.opensky.daily-limit")) {
                dailyLimitDefault = v.value();
            }
        }
        assertThat(dailyLimitDefault).isEqualTo("${aerosync.opensky.daily-limit:400}");

        TelemetryModeManager m = manager(400);
        int granted = 0;
        while (m.tryAcquireLiveRequest()) {
            granted++;
        }
        assertThat(granted).isEqualTo(400);
        assertThat(m.getStatus(0).dailyLimit()).isEqualTo(400);
    }

    @Test
    void startsLiveWithFreshQuotaWindowEndingAtNextUtcMidnight() {
        SystemStatus status = manager(400).getStatus(5);

        assertThat(status.mode()).isEqualTo(Mode.LIVE);
        assertThat(status.reason()).isEqualTo(FailoverReason.NONE);
        assertThat(status.requestsUsedToday()).isZero();
        assertThat(status.windowResetAt()).isEqualTo(Instant.parse("2026-11-28T00:00:00Z"));
        assertThat(status.activeFlightCount()).isEqualTo(5);
        assertThat(status.message()).contains("0/400");
    }

    @Test
    void exactlyLimitRequestsSucceedThenFailsOverWithQuotaExhausted() {
        TelemetryModeManager m = manager(5);
        for (int i = 0; i < 5; i++) {
            assertThat(m.tryAcquireLiveRequest()).as("request %d", i + 1).isTrue();
        }
        assertThat(m.isSimulationMode()).isFalse();

        assertThat(m.tryAcquireLiveRequest()).isFalse();

        SystemStatus status = m.getStatus(8);
        assertThat(status.mode()).isEqualTo(Mode.SIMULATION);
        assertThat(status.reason()).isEqualTo(FailoverReason.DAILY_QUOTA_EXHAUSTED);
        assertThat(status.requestsUsedToday()).isEqualTo(5);
        assertThat(status.message()).isEqualTo("Free-tier daily OpenSky quota reached (5/5). Live flight simulator active.");
        assertThat(m.tryAcquireLiveRequest()).isFalse();
        assertThat(m.getStatus(0).requestsUsedToday()).isEqualTo(5);
    }

    @Test
    void concurrentAcquiresNeverOverspendTheQuota() throws Exception {
        TelemetryModeManager m = manager(10);
        int threads = 32;
        int attemptsPerThread = 25;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger granted = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            futures.add(pool.submit(() -> {
                start.await();
                for (int i = 0; i < attemptsPerThread; i++) {
                    if (m.tryAcquireLiveRequest()) {
                        granted.incrementAndGet();
                    }
                }
                return null;
            }));
        }
        start.countDown();
        for (Future<?> f : futures) {
            f.get(10, TimeUnit.SECONDS);
        }
        pool.shutdown();

        assertThat(granted.get()).isEqualTo(10);
        assertThat(m.getStatus(0).requestsUsedToday()).isEqualTo(10);
        assertThat(m.getReason()).isEqualTo(FailoverReason.DAILY_QUOTA_EXHAUSTED);
    }

    @Test
    void quotaResetsAndReturnsToLiveAfterTheUtcDayRollsOver() {
        TelemetryModeManager m = manager(2);
        m.tryAcquireLiveRequest();
        m.tryAcquireLiveRequest();
        assertThat(m.tryAcquireLiveRequest()).isFalse();
        assertThat(m.isSimulationMode()).isTrue();

        clock.advance(Duration.ofHours(24).plusMinutes(1));

        SystemStatus afterReset = m.getStatus(0);
        assertThat(afterReset.mode()).isEqualTo(Mode.LIVE);
        assertThat(afterReset.reason()).isEqualTo(FailoverReason.NONE);
        assertThat(afterReset.requestsUsedToday()).isZero();
        assertThat(afterReset.windowResetAt()).isEqualTo(Instant.parse("2026-11-29T00:00:00Z"));
        assertThat(m.tryAcquireLiveRequest()).isTrue();
        assertThat(m.getStatus(0).requestsUsedToday()).isEqualTo(1);
    }

    @Test
    void windowResetAlsoClearsTheErrorStreak() {
        TelemetryModeManager m = manager(400);
        m.recordUpstreamError();
        m.recordUpstreamError();
        clock.set(Instant.parse("2026-11-28T00:00:01Z"));
        m.tryAcquireLiveRequest();
        assertThat(m.getConsecutiveErrors()).isZero();
    }

    @Test
    void rateLimitFailsOverImmediately() {
        TelemetryModeManager m = manager(400);
        assertThat(m.tryAcquireLiveRequest()).isTrue();

        m.recordUpstreamRateLimit();

        assertThat(m.getMode()).isEqualTo(Mode.SIMULATION);
        assertThat(m.getReason()).isEqualTo(FailoverReason.UPSTREAM_RATE_LIMITED_429);
        assertThat(m.tryAcquireLiveRequest()).isFalse();
        assertThat(m.getStatus(0).message()).contains("HTTP 429");
    }

    @Test
    void rateLimitHonoursLongerRetryAfterThenProbesLiveAgain() {
        TelemetryModeManager m = manager(400);
        m.recordUpstreamRateLimit(Duration.ofMinutes(30));

        clock.advance(Duration.ofMinutes(16));
        assertThat(m.tryAcquireLiveRequest()).as("still inside retry-after").isFalse();

        clock.advance(Duration.ofMinutes(15));
        assertThat(m.tryAcquireLiveRequest()).isTrue();
        assertThat(m.getMode()).isEqualTo(Mode.LIVE);
    }

    @Test
    void consecutiveErrorsTripUnavailableAtThreshold() {
        TelemetryModeManager m = manager(400);
        m.recordUpstreamError();
        m.recordUpstreamError();
        assertThat(m.getMode()).isEqualTo(Mode.LIVE);

        m.recordUpstreamError();

        assertThat(m.getMode()).isEqualTo(Mode.SIMULATION);
        assertThat(m.getReason()).isEqualTo(FailoverReason.UPSTREAM_UNAVAILABLE);
        assertThat(m.tryAcquireLiveRequest()).isFalse();
    }

    @Test
    void successResetsTheErrorStreak() {
        TelemetryModeManager m = manager(400);
        m.recordUpstreamError();
        m.recordUpstreamError();
        m.recordUpstreamSuccess();
        assertThat(m.getConsecutiveErrors()).isZero();

        m.recordUpstreamError();
        m.recordUpstreamError();
        assertThat(m.getMode()).isEqualTo(Mode.LIVE);
    }

    @Test
    void unavailableFailoverProbesAfterCooldownAndRetripsOnNextFailure() {
        TelemetryModeManager m = manager(400);
        m.recordUpstreamError();
        m.recordUpstreamError();
        m.recordUpstreamError();
        assertThat(m.isSimulationMode()).isTrue();

        clock.advance(Duration.ofMillis(COOLDOWN_MS));
        assertThat(m.tryAcquireLiveRequest()).isTrue();
        assertThat(m.getMode()).isEqualTo(Mode.LIVE);

        m.recordUpstreamError();
        assertThat(m.getReason()).isEqualTo(FailoverReason.UPSTREAM_UNAVAILABLE);
    }

    @Test
    void disabledBootsInManualSimulationAndNeverGrantsLiveRequests() {
        TelemetryModeManager m = new TelemetryModeManager(false, 400, 3, COOLDOWN_MS, clock);

        assertThat(m.getMode()).isEqualTo(Mode.SIMULATION);
        assertThat(m.getReason()).isEqualTo(FailoverReason.MANUAL_SIMULATION_CONFIG);
        for (int i = 0; i < 50; i++) {
            assertThat(m.tryAcquireLiveRequest()).isFalse();
        }
        clock.advance(Duration.ofDays(2));
        assertThat(m.tryAcquireLiveRequest()).isFalse();
        m.recordUpstreamRateLimit();
        m.recordUpstreamError();
        assertThat(m.getReason()).isEqualTo(FailoverReason.MANUAL_SIMULATION_CONFIG);
        assertThat(m.getStatus(0).requestsUsedToday()).isZero();
    }

    @Test
    void limitIsClampedToOpenSkyFreeTierRange() {
        assertThat(new TelemetryModeManager(true, 99_999, 3, COOLDOWN_MS, clock).getDailyLimit()).isEqualTo(4000);
        assertThat(new TelemetryModeManager(true, 0, 3, COOLDOWN_MS, clock).getDailyLimit()).isEqualTo(1);
        assertThat(new TelemetryModeManager(true, 4000, 3, COOLDOWN_MS, clock).getDailyLimit()).isEqualTo(4000);
    }

    @Test
    void negativeActiveCountIsReportedAsZero() {
        assertThat(manager(400).getStatus(-3).activeFlightCount()).isZero();
    }
}
