package com.aerosync.service;

import com.aerosync.model.FlightTelemetry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@Service
public class AviationApiClient {

    private final WebClient webClient;
    private final String apiUrl;

    public AviationApiClient(WebClient webClient, @Value("${aerosync.api.url:https://opensky-network.org/api/states/all}") String apiUrl) {
        this.webClient = webClient;
        this.apiUrl = apiUrl;
    }

    // Kept so your existing tests do not break
    public Mono<FlightTelemetry> getFlightByCallsign(String callsign) {
        return webClient.get()
                .uri(apiUrl)
                .retrieve()
                .bodyToMono(Map.class)
                .timeout(Duration.ofSeconds(5))
                .mapNotNull(response -> parseOpenSkyResponse(response, callsign))
                .onErrorResume(e -> Mono.empty());
    }

    // NEW: Fetches a batch of live flights over India
    public Flux<FlightTelemetry> getLiveFlights() {
        // Bounding box for India keeps the API response fast and lightweight
        String indiaUrl = apiUrl + "?lamin=8.0&lomin=68.0&lamax=37.0&lomax=97.0";

        return webClient.get()
                .uri(indiaUrl)
                .retrieve()
                .bodyToMono(Map.class)
                .timeout(Duration.ofSeconds(10))
                .flatMapMany(this::parseMultipleFlights)
                .onErrorResume(e -> Flux.empty());
    }

    @SuppressWarnings("unchecked")
    private FlightTelemetry parseOpenSkyResponse(Map<String, Object> response, String targetCallsign) {
        List<List<Object>> states = (List<List<Object>>) response.get("states");
        if (states == null) return null;

        for (List<Object> state : states) {
            String currentCallsign = state.get(1) != null ? state.get(1).toString().trim() : "";
            if (currentCallsign.equalsIgnoreCase(targetCallsign.trim())) {
                return mapStateToTelemetry(state);
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private Flux<FlightTelemetry> parseMultipleFlights(Map<String, Object> response) {
        List<List<Object>> states = (List<List<Object>>) response.get("states");
        if (states == null) return Flux.empty();

        return Flux.fromIterable(states)
                .map(this::mapStateToTelemetry)
                .filter(flight -> flight.latitude() != 0.0 && flight.longitude() != 0.0) // Ignore planes missing GPS
                .take(30); // Send max 30 planes to avoid overloading the frontend map
    }

    private FlightTelemetry mapStateToTelemetry(List<Object> state) {
        String callsign = state.get(1) != null ? state.get(1).toString().trim() : "UNKNOWN";
        return new FlightTelemetry(
                callsign.isEmpty() ? "UNKNOWN" : callsign,
                (String) state.get(0), // icao24
                state.get(6) != null ? ((Number) state.get(6)).doubleValue() : 0.0, // lat
                state.get(5) != null ? ((Number) state.get(5)).doubleValue() : 0.0, // lon
                state.get(7) != null ? ((Number) state.get(7)).doubleValue() : 0.0, // alt
                state.get(9) != null ? ((Number) state.get(9)).doubleValue() : 0.0, // speed
                System.currentTimeMillis()
        );
    }
}