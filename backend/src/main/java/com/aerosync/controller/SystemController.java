package com.aerosync.controller;

import com.aerosync.dto.HealthResponse;
import com.aerosync.dto.SystemStatus;
import com.aerosync.service.ActiveFlightService;
import com.aerosync.service.TelemetryModeManager;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.time.Clock;

@RestController
@RequestMapping("/api")
public class SystemController {

    private final TelemetryModeManager telemetryModeManager;
    private final ActiveFlightService activeFlightService;
    private final Clock clock;

    public SystemController(TelemetryModeManager telemetryModeManager, ActiveFlightService activeFlightService, Clock clock) {
        this.telemetryModeManager = telemetryModeManager;
        this.activeFlightService = activeFlightService;
        this.clock = clock;
    }

    /** Liveness probe for Render/Koyeb/Docker. Deliberately does not touch Kafka, Valkey or PostgreSQL. */
    @GetMapping("/health")
    public Mono<HealthResponse> health() {
        return Mono.fromSupplier(() -> new HealthResponse("UP", "aerosync-backend", clock.instant()));
    }

    @GetMapping("/system/status")
    public Mono<SystemStatus> status() {
        return activeFlightService.countActiveFlights()
                .onErrorReturn(0L)
                .defaultIfEmpty(0L)
                .map(telemetryModeManager::getStatus);
    }
}
