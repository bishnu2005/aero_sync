package com.aerosync.dto;

import java.time.Instant;

/** Uniform JSON error body for every failed API call. Never carries stack traces or driver messages. */
public record ApiErrorResponse(
        int status,
        String error,
        String message,
        String path,
        Instant timestamp
) {}
