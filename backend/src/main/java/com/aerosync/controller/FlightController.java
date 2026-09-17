package com.aerosync.controller;

import com.aerosync.model.FlightTelemetry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
@RequestMapping("/api/flights")
@CrossOrigin(origins = "http://localhost:5173")
public class FlightController {

    private final ReactiveStringRedisTemplate valkeyTemplate;
    private final ObjectMapper objectMapper;

    public FlightController(ReactiveStringRedisTemplate valkeyTemplate) {
        this.valkeyTemplate = valkeyTemplate;
        this.objectMapper = new ObjectMapper();
    }

    @GetMapping("/active")
    public Flux<FlightTelemetry> getActiveFlights() {
        return valkeyTemplate.opsForHash().values("active_flights")
                .map(jsonString -> {
                    try {
                        return objectMapper.readValue((String) jsonString, FlightTelemetry.class);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to parse Valkey cache payload", e);
                    }
                });
    }
}