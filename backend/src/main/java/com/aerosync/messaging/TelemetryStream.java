package com.aerosync.messaging;

import org.springframework.stereotype.Service;
import reactor.core.publisher.BufferOverflowStrategy;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

/**
 * Hot fan-out of telemetry JSON frames to every connected WebSocket.
 *
 * <p>{@code directBestEffort} keeps the sink alive when the last subscriber leaves (an
 * {@code onBackpressureBuffer} multicast sink would auto-cancel and stop broadcasting forever),
 * and never buffers frames while nobody is connected. Each subscriber gets its own bounded buffer
 * in {@link #getStream()}, so one slow browser cannot stall the others.
 */
@Service
public class TelemetryStream {

    static final int PER_SUBSCRIBER_BUFFER = 512;

    private final Sinks.Many<String> sink = Sinks.many().multicast().directBestEffort();

    /** Serialised so concurrent producers never hit {@code FAIL_NON_SERIALIZED}. */
    public synchronized void broadcast(String telemetryJson) {
        sink.tryEmitNext(telemetryJson);
    }

    public Flux<String> getStream() {
        return sink.asFlux().onBackpressureBuffer(PER_SUBSCRIBER_BUFFER, dropped -> { }, BufferOverflowStrategy.DROP_OLDEST);
    }

    public int subscriberCount() {
        return sink.currentSubscriberCount();
    }
}
