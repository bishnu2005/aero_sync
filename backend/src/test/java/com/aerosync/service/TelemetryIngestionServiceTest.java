package com.aerosync.service;

import com.aerosync.exception.OpenSkyRateLimitException;
import com.aerosync.exception.OpenSkyUpstreamException;
import com.aerosync.messaging.TelemetryProducer;
import com.aerosync.model.FlightTelemetry;
import com.aerosync.observability.TelemetryMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.scheduling.annotation.Scheduled;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

import static com.aerosync.support.TestFlights.flight;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class TelemetryIngestionServiceTest {

    private AviationApiClient client;
    private TelemetryProducer producer;
    private TelemetryModeManager modeManager;
    private TelemetryIngestionService service;
    private TelemetryMetrics metrics;

    @BeforeEach
    void setUp() {
        client = mock(AviationApiClient.class);
        producer = mock(TelemetryProducer.class);
        modeManager = mock(TelemetryModeManager.class);
        metrics = new TelemetryMetrics(new SimpleMeterRegistry());
        service = new TelemetryIngestionService(client, producer, modeManager, metrics);
        when(producer.publish(any())).thenReturn(Mono.empty());
    }

    @Test
    void pollIsScheduledFromConfigurableInterval() throws Exception {
        Scheduled scheduled = TelemetryIngestionService.class.getMethod("ingestFlights").getAnnotation(Scheduled.class);
        assertThat(scheduled.fixedDelayString()).isEqualTo("${aerosync.opensky.poll-interval-ms:60000}");
    }

    @Test
    void simulationModeNeverTouchesTheApi() {
        when(modeManager.tryAcquireLiveRequest()).thenReturn(false);

        service.ingestFlights().block();

        verifyNoInteractions(client);
        verifyNoInteractions(producer);
    }

    @Test
    void rateLimitRecordsFailoverAndPublishesNothing() {
        when(modeManager.tryAcquireLiveRequest()).thenReturn(true);
        when(client.getLiveFlights()).thenReturn(Flux.error(new OpenSkyRateLimitException(Duration.ofMinutes(20))));

        service.ingestFlights().block();

        verify(modeManager).recordUpstreamRateLimit(Duration.ofMinutes(20));
        verify(modeManager, never()).recordUpstreamSuccess();
        verify(modeManager, never()).recordUpstreamError();
        verifyNoInteractions(producer);
    }

    @Test
    void upstreamErrorIsRecordedAndPublishesNothing() {
        when(modeManager.tryAcquireLiveRequest()).thenReturn(true);
        when(client.getLiveFlights()).thenReturn(Flux.error(new OpenSkyUpstreamException("HTTP 503", 503)));

        service.ingestFlights().block();

        verify(modeManager).recordUpstreamError();
        verify(modeManager, never()).recordUpstreamSuccess();
        verifyNoInteractions(producer);
    }

    @Test
    void validBatchRecordsSuccessAndPublishesEveryFlight() {
        FlightTelemetry a = flight("AIC860", "800a01", 1L);
        FlightTelemetry b = flight("IGO612", "800b02", 2L);
        when(modeManager.tryAcquireLiveRequest()).thenReturn(true);
        when(client.getLiveFlights()).thenReturn(Flux.just(a, b));

        service.ingestFlights().block();

        verify(modeManager).recordUpstreamSuccess();
        verify(producer).publish(a);
        verify(producer).publish(b);
        verify(modeManager, never()).recordUpstreamError();
        assertThat(metrics.upstreamSuccesses()).isEqualTo(1);
        assertThat(metrics.publishedLive()).isEqualTo(2);
    }

    @Test
    void scheduledMethodIsReactiveAndDefersQuotaCheckToSubscription() throws Exception {
        assertThat(TelemetryIngestionService.class.getMethod("ingestFlights").getReturnType()).isEqualTo(Mono.class);
        when(modeManager.tryAcquireLiveRequest()).thenReturn(false);

        Mono<Void> tick = service.ingestFlights();
        verify(modeManager, never()).tryAcquireLiveRequest();

        tick.block();
        tick.block();
        verify(modeManager, org.mockito.Mockito.times(2)).tryAcquireLiveRequest();
    }

    @Test
    void upstreamOutcomesAreCounted() {
        when(modeManager.tryAcquireLiveRequest()).thenReturn(true);
        when(client.getLiveFlights())
                .thenReturn(Flux.error(new OpenSkyRateLimitException(null)))
                .thenReturn(Flux.error(new OpenSkyUpstreamException("HTTP 500", 500)));

        service.ingestFlights().block();
        service.ingestFlights().block();

        assertThat(metrics.upstreamRateLimits()).isEqualTo(1);
        assertThat(metrics.upstreamErrors()).isEqualTo(1);
        assertThat(metrics.publishedLive()).isZero();
    }

    @Test
    void emptyBatchStillCountsAsSuccess() {
        when(modeManager.tryAcquireLiveRequest()).thenReturn(true);
        when(client.getLiveFlights()).thenReturn(Flux.empty());

        service.ingestFlights().block();

        verify(modeManager).recordUpstreamSuccess();
        verifyNoInteractions(producer);
    }

    @Test
    void kafkaFailureIsNotMistakenForAnUpstreamFailure() {
        when(modeManager.tryAcquireLiveRequest()).thenReturn(true);
        when(client.getLiveFlights()).thenReturn(Flux.just(flight("AIC860", "800a01", 1L)));
        when(producer.publish(any())).thenReturn(Mono.error(new IllegalStateException("broker down")));

        service.ingestFlights().block(); // must not throw

        verify(modeManager).recordUpstreamSuccess();
        verify(modeManager, never()).recordUpstreamError();
    }
}
