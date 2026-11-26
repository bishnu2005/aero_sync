package com.aerosync.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.reactive.config.CorsRegistry;
import org.springframework.web.reactive.config.WebFluxConfigurer;

import java.util.Arrays;
import java.util.List;

/**
 * One CORS policy for the whole API, driven by {@code CORS_ALLOWED_ORIGINS} (comma separated).
 * Entries may be exact origins or patterns such as {@code https://*.vercel.app}. The same policy is
 * applied to the WebSocket handshake in {@link WebSocketConfig}.
 */
@Configuration
public class CorsConfig implements WebFluxConfigurer {

    private final List<String> allowedOrigins;

    public CorsConfig(@Value("${aerosync.cors.allowed-origins:http://localhost:5173}") String allowedOrigins) {
        this.allowedOrigins = Arrays.stream(allowedOrigins.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();
    }

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/api/**")
                .allowedOriginPatterns(allowedOrigins.toArray(String[]::new))
                .allowedMethods("GET", "HEAD", "OPTIONS")
                .allowedHeaders("*")
                .allowCredentials(false)
                .maxAge(3600);
    }

    public CorsConfiguration webSocketCorsConfiguration() {
        CorsConfiguration config = new CorsConfiguration();
        config.setAllowedOriginPatterns(allowedOrigins);
        config.addAllowedMethod("GET");
        config.addAllowedHeader("*");
        config.setAllowCredentials(false);
        return config;
    }

    public List<String> getAllowedOrigins() {
        return allowedOrigins;
    }
}
