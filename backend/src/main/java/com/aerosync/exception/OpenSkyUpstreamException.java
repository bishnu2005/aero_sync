package com.aerosync.exception;

/** OpenSky failed for a reason other than rate limiting: 4xx/5xx, timeout, network error, or unparseable body. */
public class OpenSkyUpstreamException extends RuntimeException {

    private final int statusCode;

    public OpenSkyUpstreamException(String message, int statusCode, Throwable cause) {
        super(message, cause);
        this.statusCode = statusCode;
    }

    public OpenSkyUpstreamException(String message, int statusCode) {
        this(message, statusCode, null);
    }

    /** HTTP status returned by OpenSky, or {@code 0} when no response was received (timeout / network). */
    public int getStatusCode() {
        return statusCode;
    }
}
