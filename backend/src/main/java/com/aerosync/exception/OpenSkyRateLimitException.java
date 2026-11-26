package com.aerosync.exception;

import java.time.Duration;

/** OpenSky answered HTTP 429 Too Many Requests. */
public class OpenSkyRateLimitException extends RuntimeException {

    private final Duration retryAfter;

    public OpenSkyRateLimitException(Duration retryAfter) {
        super("OpenSky rate limit reached (HTTP 429)");
        this.retryAfter = retryAfter;
    }

    /** Back-off requested by OpenSky's {@code X-Rate-Limit-Retry-After-Seconds} header, or {@code null}. */
    public Duration getRetryAfter() {
        return retryAfter;
    }
}
