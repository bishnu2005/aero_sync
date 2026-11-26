package com.aerosync.exception;

public class FlightNotFoundException extends RuntimeException {
    public FlightNotFoundException(String identifier) {
        super("No flight found for '" + identifier + "'.");
    }
}
