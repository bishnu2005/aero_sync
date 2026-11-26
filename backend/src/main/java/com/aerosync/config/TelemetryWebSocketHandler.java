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

    /**
     * Streams telemetry until the client goes away. Watching the inbound side means a closed browser
     * tab ends the session (and its subscription) immediately instead of on the next failed write.
     */
    @Override
    public Mono<Void> handle(WebSocketSession session) {
        Mono<Void> outbound = session.send(telemetryStream.getStream().map(session::textMessage));
        Mono<Void> inbound = session.receive().then();
        return Mono.firstWithSignal(outbound, inbound);
    }
}
