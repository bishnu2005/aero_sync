package com.aerosync.exception;

import com.aerosync.dto.ApiErrorResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.server.MethodNotAllowedException;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.ServerWebInputException;

import java.time.Clock;

/**
 * Turns every failure into an {@link ApiErrorResponse}. Messages are fixed, client-safe strings:
 * stack traces, SQL and Redis driver messages only ever go to the server log.
 *
 * <p>Since Spring Framework 6, WebFlux routes errors raised before a handler is chosen (unknown route,
 * wrong HTTP method) through {@code @ControllerAdvice} too, so 404/405 also get this JSON shape.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    private final Clock clock;

    public GlobalExceptionHandler(Clock clock) {
        this.clock = clock;
    }

    @ExceptionHandler(InvalidIdentifierException.class)
    public ResponseEntity<ApiErrorResponse> invalidIdentifier(InvalidIdentifierException e, ServerWebExchange exchange) {
        return build(HttpStatus.BAD_REQUEST, "INVALID_IDENTIFIER", e.getMessage(), exchange);
    }

    @ExceptionHandler(FlightNotFoundException.class)
    public ResponseEntity<ApiErrorResponse> flightNotFound(FlightNotFoundException e, ServerWebExchange exchange) {
        return build(HttpStatus.NOT_FOUND, "FLIGHT_NOT_FOUND", e.getMessage(), exchange);
    }

    @ExceptionHandler(ServiceUnavailableException.class)
    public ResponseEntity<ApiErrorResponse> unavailable(ServiceUnavailableException e, ServerWebExchange exchange) {
        log.error("Service unavailable on {}: {}", exchange.getRequest().getPath(), e.getMessage(), e.getCause());
        return build(HttpStatus.SERVICE_UNAVAILABLE, "SERVICE_UNAVAILABLE", e.getMessage(), exchange);
    }

    @ExceptionHandler(MethodNotAllowedException.class)
    public ResponseEntity<ApiErrorResponse> methodNotAllowed(MethodNotAllowedException e, ServerWebExchange exchange) {
        ResponseEntity<ApiErrorResponse> response = build(HttpStatus.METHOD_NOT_ALLOWED, "METHOD_NOT_ALLOWED",
                "HTTP method " + e.getHttpMethod() + " is not supported for this endpoint.", exchange);
        return ResponseEntity.status(response.getStatusCode())
                .headers(e.getHeaders())
                .contentType(MediaType.APPLICATION_JSON)
                .body(response.getBody());
    }

    @ExceptionHandler(ServerWebInputException.class)
    public ResponseEntity<ApiErrorResponse> badInput(ServerWebInputException e, ServerWebExchange exchange) {
        return build(HttpStatus.BAD_REQUEST, "BAD_REQUEST", "The request is malformed or has invalid parameters.", exchange);
    }

    @ExceptionHandler(ResponseStatusException.class)
    public ResponseEntity<ApiErrorResponse> responseStatus(ResponseStatusException e, ServerWebExchange exchange) {
        HttpStatusCode code = e.getStatusCode();
        if (code.value() == HttpStatus.NOT_FOUND.value()) {
            return build(HttpStatus.NOT_FOUND, "NOT_FOUND", "No endpoint matches this path.", exchange);
        }
        HttpStatus status = HttpStatus.resolve(code.value());
        if (status == null || status.is5xxServerError()) {
            log.error("Unhandled server error on {}", exchange.getRequest().getPath(), e);
            return build(HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", "An unexpected error occurred.", exchange);
        }
        return build(status, status.name(), status.getReasonPhrase() + ".", exchange);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiErrorResponse> unexpected(Exception e, ServerWebExchange exchange) {
        log.error("Unhandled error on {}", exchange.getRequest().getPath(), e);
        return build(HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", "An unexpected error occurred.", exchange);
    }

    private ResponseEntity<ApiErrorResponse> build(HttpStatus status, String error, String message, ServerWebExchange exchange) {
        ApiErrorResponse body = new ApiErrorResponse(status.value(), error, message,
                exchange.getRequest().getPath().value(), clock.instant());
        return ResponseEntity.status(status).contentType(MediaType.APPLICATION_JSON).body(body);
    }
}
