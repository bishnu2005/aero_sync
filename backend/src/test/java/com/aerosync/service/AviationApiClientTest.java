package com.aerosync.service;

import com.aerosync.exception.OpenSkyRateLimitException;
import com.aerosync.exception.OpenSkyUpstreamException;
import com.aerosync.model.FlightTelemetry;
import com.aerosync.support.MutableClock;
import com.aerosync.support.TestFlights;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.ExchangeFunction;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Every request is answered by a stub {@link ExchangeFunction}; nothing reaches opensky-network.org. */
class AviationApiClientTest {

    private static final String API_URL = "https://opensky.invalid/api/states/all";

    private final MutableClock clock = MutableClock.at("2026-11-27T10:00:00Z");
    private final AtomicReference<ClientRequest> lastRequest = new AtomicReference<>();
    private final AtomicInteger requestCount = new AtomicInteger();

    private AviationApiClient client(ExchangeFunction exchange) {
        return client(exchange, "", "", 2000, 150);
    }

    private AviationApiClient client(ExchangeFunction exchange, String user, String pass, long timeoutMs, int maxFlights) {
        ExchangeFunction recording = request -> {
            lastRequest.set(request);
            requestCount.incrementAndGet();
            return exchange.exchange(request);
        };
        WebClient webClient = WebClient.builder().exchangeFunction(recording).build();
        return new AviationApiClient(webClient, TestFlights.MAPPER, API_URL, user, pass, timeoutMs, maxFlights, clock);
    }

    private static ExchangeFunction respond(HttpStatus status, String body) {
        return request -> Mono.just(ClientResponse.create(status)
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .body(body)
                .build());
    }

    private static final String VALID_BODY = """
            {"time": 1764237600, "states": [
              ["800a01", "AIC860  ", "India", 1764237590, 1764237595, 72.87, 19.09, 10668.0, false, 245.5, 210.3, 0.0, null, 10700.0, "1200", false, 0],
              ["800b02", "IGO612", "India", 1764237590, 1764237598, 77.70, 13.20, 3048.0, false, 180.0, 15.0, 0.0, null, null, null, false, 0],
              ["4b1805", "SWR147  ", "Switzerland", null, 1764237599, 75.0, 25.0, null, false, null, null, null, null, 11277.6, null, false, 0]
            ]}
            """;

    @Test
    void parsesValidResponseIntoTelemetry() {
        StepVerifier.create(client(respond(HttpStatus.OK, VALID_BODY)).getLiveFlights().collectList())
                .assertNext(flights -> {
                    assertThat(flights).hasSize(3);
                    FlightTelemetry first = flights.getFirst();
                    assertThat(first.callsign()).isEqualTo("AIC860");
                    assertThat(first.icao24()).isEqualTo("800a01");
                    assertThat(first.latitude()).isEqualTo(19.09);
                    assertThat(first.longitude()).isEqualTo(72.87);
                    assertThat(first.altitude()).isEqualTo(35000.0); // 10668 m -> ft
                    assertThat(first.velocity()).isEqualTo(245.5);
                    assertThat(first.heading()).isEqualTo(210.3);
                    assertThat(first.timestamp()).isEqualTo(1764237595000L);
                    assertThat(flights).allMatch(f -> "OPENSKY".equals(f.origin()));

                    assertThat(flights.get(1).altitude()).isEqualTo(10000.0);
                    // Missing baro altitude falls back to geometric altitude.
                    assertThat(flights.get(2).altitude()).isEqualTo(37000.0);
                })
                .verifyComplete();

        assertThat(requestCount.get()).isEqualTo(1);
        assertThat(lastRequest.get().url().toString())
                .isEqualTo(API_URL + "?lamin=8.0&lomin=68.0&lamax=37.0&lomax=97.0");
        assertThat(lastRequest.get().headers().containsHeader(HttpHeaders.AUTHORIZATION)).isFalse();
    }

    @Test
    void sendsBasicAuthOnlyWhenCredentialsConfigured() {
        client(respond(HttpStatus.OK, "{\"states\":[]}"), "pilot", "s3cret", 2000, 150).getLiveFlights().blockLast();
        assertThat(lastRequest.get().headers().getFirst(HttpHeaders.AUTHORIZATION)).startsWith("Basic ");
    }

    @ParameterizedTest
    @ValueSource(strings = {"{\"time\":1,\"states\":null}", "{\"time\":1,\"states\":[]}", "{\"time\":1}"})
    void nullEmptyOrMissingStatesYieldEmptyStream(String body) {
        StepVerifier.create(client(respond(HttpStatus.OK, body)).getLiveFlights()).verifyComplete();
    }

    @Test
    void skipsInvalidEntriesWithoutFailingTheBatch() {
        String body = """
                {"states": [
                  ["aaaaaa", null, "X", 1, 1, 77.0, 20.0, 1000.0, false, 200.0, 90.0],
                  ["bbbbbb", "   ", "X", 1, 1, 77.0, 20.0, 1000.0, false, 200.0, 90.0],
                  ["cccccc", "NOLAT", "X", 1, 1, 77.0, null, 1000.0, false, 200.0, 90.0],
                  ["dddddd", "NOLON", "X", 1, 1, null, 20.0, 1000.0, false, 200.0, 90.0],
                  ["eeeeee", "TRUNC"],
                  ["ffffff", "BADLAT", "X", 1, 1, 77.0, 120.0, 1000.0, false, 200.0, 90.0],
                  ["999999", 42, "X", 1, 1, 77.0, 20.0, 1000.0, false, 200.0, 90.0],
                  "not-an-array",
                  null,
                  [null, "NOICAO", "X", 1, 1, 77.0, 20.0],
                  ["800a01", "AIC860", "India", 1, 1, 72.87, 19.09, "high", false, "fast", 210.0]
                ]}
                """;
        StepVerifier.create(client(respond(HttpStatus.OK, body)).getLiveFlights().collectList())
                .assertNext(flights -> {
                    assertThat(flights).extracting(FlightTelemetry::callsign).containsExactly("AIC860");
                    // Non-numeric altitude/velocity fall back to 0 instead of throwing ClassCastException.
                    assertThat(flights.getFirst().altitude()).isZero();
                    assertThat(flights.getFirst().velocity()).isZero();
                })
                .verifyComplete();
    }

    @Test
    void nullAltitudeVelocityHeadingAndContactDefaultSafely() {
        String body = """
                {"states": [["800a01", "AIC860", "India", null, null, 72.87, 19.09, null, false, null, null, null, null, null]]}
                """;
        StepVerifier.create(client(respond(HttpStatus.OK, body)).getLiveFlights())
                .assertNext(f -> {
                    assertThat(f.altitude()).isZero();
                    assertThat(f.velocity()).isZero();
                    assertThat(f.heading()).isZero();
                    assertThat(f.timestamp()).isEqualTo(clock.millis());
                })
                .verifyComplete();
    }

    @Test
    void capsFlightsPerPoll() {
        StepVerifier.create(client(respond(HttpStatus.OK, VALID_BODY), "", "", 2000, 2).getLiveFlights())
                .expectNextCount(2)
                .verifyComplete();
    }

    @Test
    void http429EmitsRateLimitExceptionWithRetryAfter() {
        ExchangeFunction tooMany = request -> Mono.just(ClientResponse.create(HttpStatus.TOO_MANY_REQUESTS)
                .header("X-Rate-Limit-Retry-After-Seconds", "3600")
                .body("Too many requests")
                .build());
        StepVerifier.create(client(tooMany).getLiveFlights())
                .expectErrorSatisfies(e -> {
                    assertThat(e).isInstanceOf(OpenSkyRateLimitException.class);
                    assertThat(((OpenSkyRateLimitException) e).getRetryAfter()).isEqualTo(Duration.ofHours(1));
                })
                .verify();
    }

    @Test
    void http429WithoutRetryHeaderStillRateLimits() {
        StepVerifier.create(client(respond(HttpStatus.TOO_MANY_REQUESTS, "")).getLiveFlights())
                .expectErrorSatisfies(e -> assertThat(((OpenSkyRateLimitException) e).getRetryAfter()).isNull())
                .verify();
    }

    @ParameterizedTest
    @ValueSource(ints = {400, 401, 403, 404, 500, 502, 503})
    void httpErrorsEmitTypedUpstreamException(int status) {
        StepVerifier.create(client(respond(HttpStatus.valueOf(status), "{\"error\":\"x\"}")).getLiveFlights())
                .expectErrorSatisfies(e -> {
                    assertThat(e).isInstanceOf(OpenSkyUpstreamException.class);
                    assertThat(((OpenSkyUpstreamException) e).getStatusCode()).isEqualTo(status);
                })
                .verify();
    }

    @ParameterizedTest
    @ValueSource(strings = {"{\"states\": [[\"800a01\", ", "<html>Bad Gateway</html>", "[1,2,3]", "\"just a string\"", ""})
    void malformedOrUnexpectedBodiesEmitUpstreamException(String body) {
        StepVerifier.create(client(respond(HttpStatus.OK, body)).getLiveFlights())
                .expectError(OpenSkyUpstreamException.class)
                .verify();
    }

    @Test
    void timeoutEmitsUpstreamException() {
        ExchangeFunction hang = request -> Mono.never();
        StepVerifier.create(client(hang, "", "", 150, 150).getLiveFlights())
                .expectErrorSatisfies(e -> {
                    assertThat(e).isInstanceOf(OpenSkyUpstreamException.class).hasMessageContaining("timed out");
                    assertThat(((OpenSkyUpstreamException) e).getStatusCode()).isZero();
                })
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void networkFailureEmitsUpstreamException() {
        ExchangeFunction refused = request -> Mono.error(new java.net.ConnectException("Connection refused"));
        StepVerifier.create(client(refused).getLiveFlights())
                .expectError(OpenSkyUpstreamException.class)
                .verify();
    }

    @Test
    void eachSubscriptionIsExactlyOneRequest() {
        AviationApiClient c = client(respond(HttpStatus.OK, VALID_BODY));
        List<FlightTelemetry> flights = c.getLiveFlights().collectList().block();
        assertThat(flights).hasSize(3);
        assertThat(requestCount.get()).isEqualTo(1);
    }
}
