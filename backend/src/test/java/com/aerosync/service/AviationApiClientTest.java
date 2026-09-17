package com.aerosync.service;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.test.StepVerifier;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest
class AviationApiClientTest {

    @Autowired
    private AviationApiClient apiClient;

    @Test
    void contextLoadsAndBeanExists() {
        assertNotNull(apiClient);
    }

    @Test
    void apiCallHandlesMissingOrInvalidCallsignGracefully() {
        // Will return Mono.empty() if callsign is not found in the live state vectors
        StepVerifier.create(apiClient.getFlightByCallsign("INVALID_CALLSIGN_XYZ"))
                .verifyComplete();
    }
}