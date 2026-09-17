package com.aerosync.messaging;

import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

@Service
public class TelemetryStream {

    // A multicasting sink that acts as a hot publisher for multiple WebSocket clients
    private final Sinks.Many<String> sink = Sinks.many().multicast().onBackpressureBuffer();

    public void broadcast(String telemetryJson) {
        sink.tryEmitNext(telemetryJson);
    }

    public Flux<String> getStream() {
        return sink.asFlux();
    }
}