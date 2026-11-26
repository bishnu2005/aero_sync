package com.aerosync.observability;

import com.aerosync.messaging.TelemetryStream;
import com.aerosync.service.TelemetryModeManager;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Gauges that read live state on every scrape instead of being pushed. */
@Configuration
public class ObservabilityConfig {

    @Bean
    public MeterBinder aerosyncGauges(TelemetryStream telemetryStream, TelemetryModeManager modeManager) {
        return registry -> {
            Gauge.builder("aerosync.websocket.subscribers", telemetryStream, TelemetryStream::subscriberCount)
                    .description("Connected WebSocket telemetry subscribers")
                    .register(registry);
            Gauge.builder("aerosync.opensky.quota.used", modeManager, m -> m.getStatus(0).requestsUsedToday())
                    .description("OpenSky requests used in the current UTC day")
                    .register(registry);
            Gauge.builder("aerosync.opensky.quota.limit", modeManager, TelemetryModeManager::getDailyLimit)
                    .description("OpenSky daily request budget")
                    .register(registry);
            Gauge.builder("aerosync.mode.simulation", modeManager, m -> m.isSimulationMode() ? 1 : 0)
                    .description("1 while the simulated fallback feed is active, 0 for the live feed")
                    .register(registry);
        };
    }
}
