package com.aerosync.messaging;

import com.aerosync.model.FlightTelemetry;
import com.aerosync.observability.TelemetryMetrics;
import com.aerosync.service.TelemetryModeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Synthetic radar used whenever {@link TelemetryModeManager} is in simulation mode (quota exhausted,
 * OpenSky down, or {@code OPENSKY_ENABLED=false}). Always registered; each tick is a no-op in live mode.
 *
 * <p>Eight aircraft fly back and forth along real Indian corridors at realistic cruise altitude and
 * speed. Every update goes through {@link TelemetryProducer}, so the simulator exercises the same
 * Kafka -> consumer -> Valkey/PostgreSQL/WebSocket path as live data.
 */
@Component
public class FlightSimulator {

    private static final Logger log = LoggerFactory.getLogger(FlightSimulator.class);
    private static final double EARTH_RADIUS_M = 6_371_000;

    record Corridor(String callsign, String icao24, String route,
                    double fromLat, double fromLon, double toLat, double toLon,
                    double cruiseAltitudeFt, double velocityMps) {}

    static final List<Corridor> CORRIDORS = List.of(
            new Corridor("AIC860", "800a01", "Delhi -> Mumbai", 28.5562, 77.1000, 19.0896, 72.8656, 35000, 245),
            new Corridor("IGO612", "800b02", "Bengaluru -> Delhi", 13.1986, 77.7066, 28.5562, 77.1000, 37000, 250),
            new Corridor("VTI941", "800c03", "Mumbai -> Kolkata", 19.0896, 72.8656, 22.6547, 88.4467, 36000, 240),
            new Corridor("AXB415", "800d04", "Chennai -> Hyderabad", 12.9941, 80.1709, 17.2403, 78.4294, 29000, 215),
            new Corridor("AKJ204", "800e05", "Bengaluru -> Goa", 13.1986, 77.7066, 15.3808, 73.8314, 28000, 210),
            new Corridor("AIC101", "800f06", "Delhi -> Bengaluru", 28.5562, 77.1000, 13.1986, 77.7066, 38000, 255),
            new Corridor("IGO408", "801007", "Hyderabad -> Ahmedabad", 17.2403, 78.4294, 23.0772, 72.6347, 33000, 230),
            new Corridor("SIA423", "801108", "Mumbai -> Chennai", 19.0896, 72.8656, 12.9941, 80.1709, 34000, 260)
    );

    private final TelemetryProducer producer;
    private final TelemetryModeManager modeManager;
    private final Clock clock;
    private final TelemetryMetrics metrics;
    private final double secondsPerTick;

    // Guarded by "this".
    private final double[] progress = new double[CORRIDORS.size()];
    private final boolean[] outbound = new boolean[CORRIDORS.size()];
    private long tick;

    public FlightSimulator(TelemetryProducer producer,
                           TelemetryModeManager modeManager,
                           Clock clock,
                           TelemetryMetrics metrics,
                           @Value("${aerosync.simulator.interval-ms:3000}") long intervalMs,
                           @Value("${aerosync.simulator.time-scale:4.0}") double timeScale) {
        this.producer = producer;
        this.modeManager = modeManager;
        this.clock = clock;
        this.metrics = metrics;
        this.secondsPerTick = Math.max(1, intervalMs) / 1000.0 * Math.max(0.1, timeScale);
        for (int i = 0; i < CORRIDORS.size(); i++) {
            progress[i] = (0.08 + i * 0.11) % 1.0; // spread aircraft along their routes
            outbound[i] = i % 2 == 0;
        }
    }

    @Scheduled(fixedDelayString = "${aerosync.simulator.interval-ms:3000}",
            initialDelayString = "${aerosync.simulator.initial-delay-ms:2000}")
    /** Reactive {@code @Scheduled}: the next tick is scheduled only after this frame reached Kafka. */
    public Mono<Void> tick() {
        return Mono.defer(() -> {
            if (!modeManager.isSimulationMode()) {
                return Mono.empty();
            }
            List<FlightTelemetry> frame = advance();
            return Flux.fromIterable(frame)
                    .concatMap(producer::publish)
                    .then()
                    .timeout(Duration.ofSeconds(30))
                    .doOnSuccess(v -> metrics.published(TelemetryMetrics.Source.SIMULATED, frame.size()))
                    .onErrorResume(e -> {
                        log.error("Simulator tick failed: {}", e.getMessage());
                        return Mono.empty();
                    });
        });
    }

    /** Moves every aircraft one tick along its corridor and returns the new positions. */
    synchronized List<FlightTelemetry> advance() {
        tick++;
        long now = clock.millis();
        List<FlightTelemetry> frame = new ArrayList<>(CORRIDORS.size());
        for (int i = 0; i < CORRIDORS.size(); i++) {
            Corridor c = CORRIDORS.get(i);
            double length = distanceMeters(c.fromLat(), c.fromLon(), c.toLat(), c.toLon());
            double step = c.velocityMps() * secondsPerTick / length;

            double p = progress[i] + (outbound[i] ? step : -step);
            if (p >= 1.0) {
                p = 1.0;
                outbound[i] = false;
            } else if (p <= 0.0) {
                p = 0.0;
                outbound[i] = true;
            }
            progress[i] = p;

            double lat = c.fromLat() + (c.toLat() - c.fromLat()) * p;
            double lon = c.fromLon() + (c.toLon() - c.fromLon()) * p;
            double heading = outbound[i]
                    ? bearing(lat, lon, c.toLat(), c.toLon())
                    : bearing(lat, lon, c.fromLat(), c.fromLon());
            double altitude = Math.round(c.cruiseAltitudeFt() + 150 * Math.sin(tick * 0.05 + i));
            double velocity = Math.round((c.velocityMps() + 2 * Math.sin(tick * 0.07 + i)) * 10) / 10.0;

            frame.add(new FlightTelemetry(c.callsign(), c.icao24(), lat, lon, altitude, velocity, heading, now,
                    FlightTelemetry.ORIGIN_SIMULATOR));
        }
        return frame;
    }

    static double distanceMeters(double lat1, double lon1, double lat2, double lon2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        return 2 * EARTH_RADIUS_M * Math.asin(Math.min(1.0, Math.sqrt(a)));
    }

    static double bearing(double lat1, double lon1, double lat2, double lon2) {
        double phi1 = Math.toRadians(lat1);
        double phi2 = Math.toRadians(lat2);
        double dLon = Math.toRadians(lon2 - lon1);
        double y = Math.sin(dLon) * Math.cos(phi2);
        double x = Math.cos(phi1) * Math.sin(phi2) - Math.sin(phi1) * Math.cos(phi2) * Math.cos(dLon);
        double degrees = Math.toDegrees(Math.atan2(y, x));
        return Math.round(((degrees + 360) % 360) * 10) / 10.0;
    }
}
