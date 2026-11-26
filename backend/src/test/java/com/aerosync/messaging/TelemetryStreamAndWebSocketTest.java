package com.aerosync.messaging;

import com.aerosync.config.TelemetryWebSocketHandler;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TelemetryStreamAndWebSocketTest {

    private final TelemetryStream stream = new TelemetryStream();

    @Test
    void multicastsEveryFrameToAllSubscribers() {
        List<String> first = new CopyOnWriteArrayList<>();
        List<String> second = new CopyOnWriteArrayList<>();
        Disposable a = stream.getStream().subscribe(first::add);
        Disposable b = stream.getStream().subscribe(second::add);

        stream.broadcast("f1");
        stream.broadcast("f2");

        assertThat(first).containsExactly("f1", "f2");
        assertThat(second).containsExactly("f1", "f2");
        a.dispose();
        b.dispose();
    }

    @Test
    void stepVerifierSeesFramesInOrder() {
        StepVerifier.create(stream.getStream().take(3))
                .then(() -> {
                    stream.broadcast("a");
                    stream.broadcast("b");
                    stream.broadcast("c");
                })
                .expectNext("a", "b", "c")
                .verifyComplete();
    }

    @Test
    void cancellingOneSubscriberLeavesTheOthersStreaming() {
        List<String> survivor = new CopyOnWriteArrayList<>();
        Disposable leaving = stream.getStream().subscribe();
        Disposable staying = stream.getStream().subscribe(survivor::add);
        assertThat(stream.subscriberCount()).isEqualTo(2);

        leaving.dispose();
        stream.broadcast("after-cancel");

        assertThat(survivor).containsExactly("after-cancel");
        assertThat(stream.subscriberCount()).isEqualTo(1);
        staying.dispose();
    }

    @Test
    void streamSurvivesEveryoneDisconnectingAndDoesNotReplayOldFrames() {
        stream.getStream().subscribe().dispose();
        stream.broadcast("nobody-listening");

        StepVerifier.create(stream.getStream().take(1))
                .then(() -> stream.broadcast("fresh"))
                .expectNext("fresh")
                .verifyComplete();
    }

    @Test
    void rapidBurstsAreDeliveredWithoutErrors() {
        List<String> received = new CopyOnWriteArrayList<>();
        Disposable sub = stream.getStream().subscribe(received::add);

        IntStream.range(0, 10_000).parallel().forEach(i -> stream.broadcast("m" + i));

        assertThat(received).hasSize(10_000);
        sub.dispose();
    }

    @Test
    void slowSubscriberDropsOldestInsteadOfBlockingOthers() {
        List<String> fast = new CopyOnWriteArrayList<>();
        Disposable fastSub = stream.getStream().subscribe(fast::add);
        // A subscriber that never requests anything.
        StepVerifier.create(stream.getStream(), 0)
                .then(() -> IntStream.range(0, 2_000).forEach(i -> stream.broadcast("m" + i)))
                .thenRequest(1)
                .assertNext(first -> assertThat(first).isNotEqualTo("m0"))
                .thenCancel()
                .verify(Duration.ofSeconds(5));

        assertThat(fast).hasSize(2_000);
        fastSub.dispose();
    }

    @Test
    void webSocketHandlerStreamsFramesUntilClientCloses() {
        TelemetryWebSocketHandler handler = new TelemetryWebSocketHandler(stream);
        WebSocketSession session = mock(WebSocketSession.class);
        Sinks.Empty<Void> clientClosed = Sinks.empty();
        List<String> sent = new CopyOnWriteArrayList<>();

        when(session.receive()).thenReturn(clientClosed.asMono().thenMany(Flux.<WebSocketMessage>empty()));
        when(session.textMessage(anyString())).thenAnswer(inv -> new WebSocketMessage(WebSocketMessage.Type.TEXT,
                DefaultDataBufferFactory.sharedInstance.wrap(inv.<String>getArgument(0).getBytes(StandardCharsets.UTF_8))));
        when(session.send(any())).thenAnswer(inv -> {
            Flux<WebSocketMessage> out = Flux.from(inv.getArgument(0));
            return out.doOnNext(m -> sent.add(m.getPayloadAsText())).then();
        });

        Disposable running = handler.handle(session).subscribe();
        await().atMost(Duration.ofSeconds(2)).until(() -> stream.subscriberCount() == 1);

        stream.broadcast("{\"callsign\":\"AIC860\"}");
        assertThat(sent).containsExactly("{\"callsign\":\"AIC860\"}");

        clientClosed.tryEmitEmpty();
        await().atMost(Duration.ofSeconds(2)).until(() -> stream.subscriberCount() == 0);
        assertThat(running.isDisposed()).isTrue();

        stream.broadcast("after-close"); // must not fail with no subscribers
        assertThat(sent).hasSize(1);
    }

    @Test
    void webSocketHandlerEndsWhenOutboundFails() {
        TelemetryWebSocketHandler handler = new TelemetryWebSocketHandler(stream);
        WebSocketSession session = mock(WebSocketSession.class);
        when(session.receive()).thenReturn(Flux.never());
        when(session.send(any())).thenReturn(Mono.error(new IllegalStateException("broken pipe")));

        StepVerifier.create(handler.handle(session)).expectError(IllegalStateException.class).verify(Duration.ofSeconds(2));
    }
}
