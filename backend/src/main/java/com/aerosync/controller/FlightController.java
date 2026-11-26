package com.aerosync.controller;

import com.aerosync.dto.FlightDetailResponse;
import com.aerosync.exception.InvalidIdentifierException;
import com.aerosync.model.FlightTelemetry;
import com.aerosync.service.ActiveFlightService;
import com.aerosync.service.FlightLookupService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.regex.Pattern;

@RestController
@RequestMapping("/api/flights")
public class FlightController {

    static final int MAX_IDENTIFIER_LENGTH = 32;
    private static final Pattern IDENTIFIER = Pattern.compile("^[A-Za-z0-9_-]{1,32}$");

    private final ActiveFlightService activeFlightService;
    private final FlightLookupService flightLookupService;

    public FlightController(ActiveFlightService activeFlightService, FlightLookupService flightLookupService) {
        this.activeFlightService = activeFlightService;
        this.flightLookupService = flightLookupService;
    }

    @GetMapping("/active")
    public Flux<FlightTelemetry> getActiveFlights() {
        return activeFlightService.getActiveFlights();
    }

    /** Flight ID / Callsign lookup by callsign (e.g. {@code AIC860}) or icao24 (e.g. {@code 800a01}). */
    @GetMapping("/{identifier}")
    public Mono<FlightDetailResponse> getFlight(@PathVariable String identifier) {
        return Mono.fromCallable(() -> validate(identifier)).flatMap(flightLookupService::lookup);
    }

    static String validate(String raw) {
        String identifier = raw == null ? "" : raw.trim();
        if (identifier.isEmpty()) {
            throw new InvalidIdentifierException("Flight identifier must not be blank.");
        }
        if (identifier.length() > MAX_IDENTIFIER_LENGTH) {
            throw new InvalidIdentifierException("Flight identifier must be at most " + MAX_IDENTIFIER_LENGTH + " characters.");
        }
        if (!IDENTIFIER.matcher(identifier).matches()) {
            throw new InvalidIdentifierException("Flight identifier may only contain letters, digits, '_' and '-'.");
        }
        return identifier;
    }
}
