package com.aerosync.config;

import com.aerosync.messaging.TelemetryStream;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;

@Component
public class TelemetryWebSocketHandler implements WebSocketHandler {

    private final TelemetryStream telemetryStream;

    public TelemetryWebSocketHandler(TelemetryStream telemetryStream) {
        this.telemetryStream = telemetryStream;
    }

    @Override
    public Mono<Void> handle(WebSocketSession session) {
        return session.send(
                telemetryStream.getStream()
                        .map(session::textMessage)
        );
    }
}