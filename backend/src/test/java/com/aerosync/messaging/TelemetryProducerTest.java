package com.aerosync.messaging;

import com.aerosync.model.FlightTelemetry;
import com.aerosync.observability.TelemetryMetrics;
import com.aerosync.support.TestFlights;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import reactor.test.StepVerifier;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static com.aerosync.support.TestFlights.flight;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TelemetryProducerTest {

    private KafkaTemplate<String, String> kafkaTemplate;
    private TelemetryMetrics metrics;
    private TelemetryProducer producer;
    private final AtomicReference<String> sendThread = new AtomicReference<>();

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        kafkaTemplate = mock(KafkaTemplate.class);
        metrics = new TelemetryMetrics(new SimpleMeterRegistry());
        producer = new TelemetryProducer(kafkaTemplate, TestFlights.MAPPER, metrics);
    }

    @Test
    void sendIsDeferredAndRunsOffTheCallingThreadOnBoundedElastic() {
        when(kafkaTemplate.send(eq(TelemetryProducer.TOPIC), eq("AIC860"), anyString())).thenAnswer(inv -> {
            sendThread.set(Thread.currentThread().getName());
            return CompletableFuture.completedFuture(mock(SendResult.class));
        });

        var mono = producer.publish(flight("AIC860", "800a01", 1L));
        verify(kafkaTemplate, never()).send(anyString(), anyString(), anyString()); // lazy until subscribed

        StepVerifier.create(mono).verifyComplete();
        assertThat(sendThread.get()).startsWith("boundedElastic");
        assertThat(metrics.kafkaPublishFailures()).isZero();
    }

    @Test
    void payloadIsKeyedByCallsignAndSerialisedAsJson() {
        AtomicReference<String> payload = new AtomicReference<>();
        when(kafkaTemplate.send(eq(TelemetryProducer.TOPIC), eq("AIC860"), anyString())).thenAnswer(inv -> {
            payload.set(inv.getArgument(2));
            return CompletableFuture.completedFuture(mock(SendResult.class));
        });

        producer.publish(flight("AIC860", "800a01", 42L)).block();

        FlightTelemetry decoded = TestFlights.MAPPER.readValue(payload.get(), FlightTelemetry.class);
        assertThat(decoded).isEqualTo(flight("AIC860", "800a01", 42L));
    }

    @Test
    void brokerFailureIsSwallowedAndCounted() {
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker down")));

        StepVerifier.create(producer.publish(flight("AIC860", "800a01", 1L))).verifyComplete();
        assertThat(metrics.kafkaPublishFailures()).isEqualTo(1);
    }

    @Test
    void synchronousSendExceptionIsSwallowedAndCounted() {
        when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenThrow(new IllegalStateException("max.block.ms exceeded"));

        StepVerifier.create(producer.publish(flight("AIC860", "800a01", 1L))).verifyComplete();
        assertThat(metrics.kafkaPublishFailures()).isEqualTo(1);
    }
}
