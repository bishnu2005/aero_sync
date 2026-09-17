package com.aerosync.service;

import com.aerosync.messaging.TelemetryProducer;
import com.aerosync.model.FlightTelemetry;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TelemetryIngestionServiceTest {

    @Test
    void ingestFlightsSuccessfullyFetchesAndPublishes() {
        // 1. Mock the dependencies
        AviationApiClient mockClient = Mockito.mock(AviationApiClient.class);
        TelemetryProducer mockProducer = Mockito.mock(TelemetryProducer.class);
        TelemetryIngestionService service = new TelemetryIngestionService(mockClient, mockProducer);

        FlightTelemetry dummyTelemetry = new FlightTelemetry(
                "AIC864", "123456", 12.0, 77.0, 10000.0, 250.0, System.currentTimeMillis()
        );

        // 2. Define the mock behavior for our new batch method
        when(mockClient.getLiveFlights()).thenReturn(Flux.just(dummyTelemetry));
        when(mockProducer.publish(any(FlightTelemetry.class))).thenReturn(Mono.empty());

        // 3. Call the newly scheduled method directly
        service.ingestFlights();

        // 4. Verify the API was called and the data was pushed to Kafka
        verify(mockClient, times(1)).getLiveFlights();
        verify(mockProducer, times(1)).publish(dummyTelemetry);
    }
}