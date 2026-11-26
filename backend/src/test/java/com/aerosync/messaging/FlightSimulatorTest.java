package com.aerosync.messaging;

import com.aerosync.model.FlightTelemetry;
import com.aerosync.service.TelemetryModeManager;
import com.aerosync.support.MutableClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class FlightSimulatorTest {

    private final MutableClock clock = MutableClock.at("2026-11-27T10:00:00Z");
    private TelemetryProducer producer;
    private TelemetryModeManager modeManager;
    private FlightSimulator simulator;

    @BeforeEach
    void setUp() {
        producer = mock(TelemetryProducer.class);
        modeManager = mock(TelemetryModeManager.class);
        when(producer.publish(any())).thenReturn(Mono.empty());
        simulator = new FlightSimulator(producer, modeManager, clock, 3000, 4.0);
    }

    @Test
    void isAlwaysAComponentScheduledFromConfig() throws Exception {
        assertThat(FlightSimulator.class.isAnnotationPresent(Component.class)).isTrue();
        Scheduled scheduled = FlightSimulator.class.getMethod("tick").getAnnotation(Scheduled.class);
        assertThat(scheduled.fixedDelayString()).isEqualTo("${aerosync.simulator.interval-ms:3000}");
    }

    @Test
    void doesNothingInLiveMode() {
        when(modeManager.isSimulationMode()).thenReturn(false);

        simulator.tick();

        verifyNoInteractions(producer);
    }

    @Test
    void publishesEightFlightsThroughKafkaInSimulationMode() {
        when(modeManager.isSimulationMode()).thenReturn(true);

        simulator.tick();

        verify(producer, times(8)).publish(any(FlightTelemetry.class));
    }

    @Test
    void kafkaFailureDoesNotBreakTheTick() {
        when(modeManager.isSimulationMode()).thenReturn(true);
        when(producer.publish(any())).thenReturn(Mono.error(new IllegalStateException("broker down")));

        simulator.tick(); // must not throw
    }

    @Test
    void flightsAreDistinctAndRealistic() {
        List<FlightTelemetry> frame = simulator.advance();

        assertThat(frame).hasSize(8);
        assertThat(frame).extracting(FlightTelemetry::callsign)
                .containsExactly("AIC860", "IGO612", "VTI941", "AXB415", "AKJ204", "AIC101", "IGO408", "SIA423");
        Set<String> icaos = new HashSet<>();
        for (FlightTelemetry f : frame) {
            assertThat(f.icao24()).matches("^[0-9a-f]{6}$");
            icaos.add(f.icao24());
            assertThat(f.altitude()).isBetween(27_800.0, 38_200.0);
            assertThat(f.velocity()).isBetween(208.0, 262.0);
            assertThat(f.heading()).isBetween(0.0, 360.0);
            assertThat(f.timestamp()).isEqualTo(clock.millis());
        }
        assertThat(icaos).hasSize(8);
    }

    @Test
    void aircraftMoveSmoothlyAndNeverLeaveTheirCorridor() {
        List<FlightTelemetry> previous = simulator.advance();
        for (int tick = 0; tick < 20_000; tick++) {
            List<FlightTelemetry> frame = simulator.advance();
            for (int i = 0; i < frame.size(); i++) {
                FlightSimulator.Corridor c = FlightSimulator.CORRIDORS.get(i);
                FlightTelemetry f = frame.get(i);
                assertThat(f.latitude()).isBetween(Math.min(c.fromLat(), c.toLat()) - 1e-9, Math.max(c.fromLat(), c.toLat()) + 1e-9);
                assertThat(f.longitude()).isBetween(Math.min(c.fromLon(), c.toLon()) - 1e-9, Math.max(c.fromLon(), c.toLon()) + 1e-9);
                double jump = FlightSimulator.distanceMeters(previous.get(i).latitude(), previous.get(i).longitude(),
                        f.latitude(), f.longitude());
                assertThat(jump).as("per-tick movement of %s", f.callsign()).isLessThan(5_000);
            }
            previous = frame;
        }
    }

    @Test
    void bearingMath() {
        assertThat(FlightSimulator.bearing(0, 0, 1, 0)).isEqualTo(0.0);
        assertThat(FlightSimulator.bearing(0, 0, 0, 1)).isEqualTo(90.0);
        assertThat(FlightSimulator.bearing(1, 0, 0, 0)).isEqualTo(180.0);
        assertThat(FlightSimulator.bearing(0, 1, 0, 0)).isEqualTo(270.0);
        assertThat(FlightSimulator.distanceMeters(28.5562, 77.1, 19.0896, 72.8656)).isBetween(1_100_000.0, 1_160_000.0);
    }
}
