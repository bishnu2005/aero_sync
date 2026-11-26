package com.aerosync.service;

import com.aerosync.exception.OpenSkyRateLimitException;
import com.aerosync.exception.OpenSkyUpstreamException;
import com.aerosync.model.FlightTelemetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * Reads live state vectors for the Indian airspace from the OpenSky Network REST API.
 *
 * <p>Errors are surfaced as typed exceptions so the caller can drive the failover state machine:
 * {@link OpenSkyRateLimitException} for HTTP 429 and {@link OpenSkyUpstreamException} for anything
 * else (4xx/5xx, timeouts, network errors, unparseable bodies).
 */
@Service
public class AviationApiClient {

    private static final Logger log = LoggerFactory.getLogger(AviationApiClient.class);

    /** India bounding box. */
    static final String INDIA_BOUNDING_BOX = "lamin=8.0&lomin=68.0&lamax=37.0&lomax=97.0";
    static final String RETRY_AFTER_HEADER = "X-Rate-Limit-Retry-After-Seconds";
    private static final double METERS_TO_FEET = 3.28084;

    // OpenSky state vector indexes: https://openskynetwork.github.io/opensky-api/rest.html
    private static final int ICAO24 = 0;
    private static final int CALLSIGN = 1;
    private static final int LAST_CONTACT = 4;
    private static final int LONGITUDE = 5;
    private static final int LATITUDE = 6;
    private static final int BARO_ALTITUDE = 7;
    private static final int VELOCITY = 9;
    private static final int TRUE_TRACK = 10;
    private static final int GEO_ALTITUDE = 13;

    private final WebClient webClient;
    private final ObjectMapper objectMapper;
    private final String apiUrl;
    private final String username;
    private final String password;
    private final Duration timeout;
    private final int maxFlightsPerPoll;
    private final Clock clock;

    public AviationApiClient(
            @Qualifier("openSkyWebClient") WebClient webClient,
            ObjectMapper objectMapper,
            @Value("${aerosync.opensky.api-url:https://opensky-network.org/api/states/all}") String apiUrl,
            @Value("${aerosync.opensky.username:}") String username,
            @Value("${aerosync.opensky.password:}") String password,
            @Value("${aerosync.opensky.timeout-ms:8000}") long timeoutMs,
            @Value("${aerosync.opensky.max-flights-per-poll:150}") int maxFlightsPerPoll,
            Clock clock) {
        this.webClient = webClient;
        this.objectMapper = objectMapper;
        this.apiUrl = apiUrl;
        this.username = username;
        this.password = password;
        this.timeout = Duration.ofMillis(timeoutMs);
        this.maxFlightsPerPoll = Math.max(1, maxFlightsPerPoll);
        this.clock = clock;
    }

    /** Exactly one OpenSky HTTP request per subscription. */
    public Flux<FlightTelemetry> getLiveFlights() {
        return webClient.get()
                .uri(apiUrl + "?" + INDIA_BOUNDING_BOX)
                .accept(MediaType.APPLICATION_JSON)
                .headers(headers -> {
                    if (StringUtils.hasText(username)) {
                        headers.setBasicAuth(username, password == null ? "" : password);
                    }
                })
                .exchangeToMono(this::readBody)
                .timeout(timeout)
                .onErrorMap(AviationApiClient::translate)
                .flatMapMany(body -> Mono.fromCallable(() -> parseStates(body)).flatMapIterable(list -> list))
                .take(maxFlightsPerPoll);
    }

    private Mono<String> readBody(ClientResponse response) {
        HttpStatusCode status = response.statusCode();
        if (status.value() == 429) {
            Duration retryAfter = parseRetryAfter(response.headers().header(RETRY_AFTER_HEADER));
            return response.releaseBody().then(Mono.error(new OpenSkyRateLimitException(retryAfter)));
        }
        if (status.isError()) {
            return response.releaseBody().then(Mono.error(
                    new OpenSkyUpstreamException("OpenSky responded with HTTP " + status.value(), status.value())));
        }
        return response.bodyToMono(String.class).defaultIfEmpty("");
    }

    private static Duration parseRetryAfter(List<String> values) {
        if (values.isEmpty()) {
            return null;
        }
        try {
            long seconds = Long.parseLong(values.getFirst().trim());
            return seconds > 0 ? Duration.ofSeconds(seconds) : null;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Throwable translate(Throwable error) {
        if (error instanceof OpenSkyRateLimitException || error instanceof OpenSkyUpstreamException) {
            return error;
        }
        if (error instanceof TimeoutException) {
            return new OpenSkyUpstreamException("OpenSky request timed out", 0, error);
        }
        return new OpenSkyUpstreamException("OpenSky request failed: " + error.getClass().getSimpleName(), 0, error);
    }

    List<FlightTelemetry> parseStates(String body) {
        JsonNode root;
        try {
            root = objectMapper.readTree(body);
        } catch (JacksonException e) {
            throw new OpenSkyUpstreamException("OpenSky returned malformed JSON", 200, e);
        }
        if (root == null || !root.isObject()) {
            throw new OpenSkyUpstreamException("OpenSky returned an unexpected payload", 200);
        }
        JsonNode states = root.get("states");
        if (states == null || !states.isArray()) {
            return List.of();
        }
        List<FlightTelemetry> flights = new ArrayList<>(states.size());
        int skipped = 0;
        for (JsonNode state : states) {
            FlightTelemetry flight = mapState(state);
            if (flight != null) {
                flights.add(flight);
            } else {
                skipped++;
            }
        }
        if (skipped > 0) {
            log.debug("Skipped {} OpenSky state vectors without a usable callsign or position.", skipped);
        }
        return flights;
    }

    private FlightTelemetry mapState(JsonNode state) {
        if (state == null || !state.isArray()) {
            return null;
        }
        String icao24 = text(state, ICAO24);
        String callsign = text(state, CALLSIGN);
        Double latitude = number(state, LATITUDE);
        Double longitude = number(state, LONGITUDE);
        if (icao24 == null || callsign == null || latitude == null || longitude == null
                || latitude < -90 || latitude > 90 || longitude < -180 || longitude > 180) {
            return null;
        }
        Double altitudeMeters = number(state, BARO_ALTITUDE);
        if (altitudeMeters == null) {
            altitudeMeters = number(state, GEO_ALTITUDE);
        }
        Double velocity = number(state, VELOCITY);
        Double heading = number(state, TRUE_TRACK);
        Double lastContact = number(state, LAST_CONTACT);

        return new FlightTelemetry(
                callsign,
                icao24.toLowerCase(),
                latitude,
                longitude,
                altitudeMeters != null ? Math.round(altitudeMeters * METERS_TO_FEET) : 0.0,
                velocity != null ? velocity : 0.0,
                heading != null ? heading : 0.0,
                lastContact != null ? (long) (lastContact * 1000) : clock.millis(),
                FlightTelemetry.ORIGIN_OPENSKY
        );
    }

    private static String text(JsonNode array, int index) {
        JsonNode node = array.get(index);
        if (node == null || !node.isString()) {
            return null;
        }
        String value = node.stringValue().trim();
        return value.isEmpty() ? null : value;
    }

    private static Double number(JsonNode array, int index) {
        JsonNode node = array.get(index);
        if (node == null || !node.isNumber()) {
            return null;
        }
        double value = node.doubleValue();
        return Double.isFinite(value) ? value : null;
    }
}
