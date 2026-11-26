package com.aerosync.exception;

/** Raised when every backing store needed to answer a request is unreachable. */
public class ServiceUnavailableException extends RuntimeException {
    public ServiceUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
