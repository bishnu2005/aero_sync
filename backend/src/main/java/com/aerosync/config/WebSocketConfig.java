package com.aerosync.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.HandlerMapping;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter;

import java.util.Map;

@Configuration
public class WebSocketConfig {

    public static final String TELEMETRY_PATH = "/ws/telemetry";

    @Bean
    public HandlerMapping webSocketMapping(TelemetryWebSocketHandler telemetryWebSocketHandler, CorsConfig corsConfig) {
        SimpleUrlHandlerMapping mapping = new SimpleUrlHandlerMapping();
        mapping.setOrder(1);
        mapping.setUrlMap(Map.of(TELEMETRY_PATH, telemetryWebSocketHandler));
        // Browsers do not apply CORS to WebSockets, so the handshake's Origin is checked here.
        mapping.setCorsConfigurations(Map.of(TELEMETRY_PATH, corsConfig.webSocketCorsConfiguration()));
        return mapping;
    }

    @Bean
    public WebSocketHandlerAdapter handlerAdapter() {
        return new WebSocketHandlerAdapter();
    }
}
