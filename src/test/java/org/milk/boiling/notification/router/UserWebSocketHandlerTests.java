package org.milk.boiling.notification.router;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.milk.boiling.notification.dto.MilkCommandType;
import org.milk.boiling.notification.dto.MilkSubDTO;
import org.milk.boiling.notification.dto.SerializationService;
import org.milk.boiling.notification.dto.UserSubscriptions;
import org.milk.boiling.notification.dto.websocket.UserWebSocketCommand;
import org.milk.boiling.notification.router.websocket.SubscribeCommandDispatcher;
import org.milk.boiling.notification.router.websocket.UnsubscribeCommandDispatcher;
import org.milk.boiling.notification.router.websocket.WebSocketCommandDispatchService;
import org.milk.boiling.notification.router.websocket.WebSocketSubscriptionService;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class UserWebSocketHandlerTests {
    private final SerializationService serialization = mock(SerializationService.class);
    private final WebSocketSession session = mock(WebSocketSession.class);
    private final WebSocketSubscriptionService subscriptions = new WebSocketSubscriptionService();
    private final UUID userId = UUID.randomUUID();
    private UserWebSocketHandler handler;

    @BeforeEach
    void setUp() {
        WebSocketCommandDispatchService dispatcher = new WebSocketCommandDispatchService(List.of(
                new SubscribeCommandDispatcher(subscriptions), new UnsubscribeCommandDispatcher(subscriptions)));
        handler = new UserWebSocketHandler("/test-router", serialization, dispatcher, subscriptions);
        when(serialization.serialize(any())).thenReturn(new byte[]{0});
        when(session.send(any())).thenReturn(Mono.empty());
    }

    @Test
    void usesConfiguredPath() {
        assertThat(handler.getPath()).isEqualTo("/test-router");
    }

    @Test
    void dispatchesReceivedSubscription() {
        when(serialization.deserialize(new byte[]{1})).thenReturn(command(MilkCommandType.SUBSCRIBE));
        when(session.receive()).thenReturn(Flux.just(message(1)));

        StepVerifier.create(handler.handle(session)).verifyComplete();

        assertThat(subscriptions.getSessions(userId)).containsExactly(session);
    }

    @Test
    void processesSubscribeAndUnsubscribeInOrder() {
        when(serialization.deserialize(new byte[]{1})).thenReturn(command(MilkCommandType.SUBSCRIBE));
        when(serialization.deserialize(new byte[]{2})).thenReturn(command(MilkCommandType.UNSUBSCRIBE));
        when(session.receive()).thenReturn(Flux.just(message(1), message(2)));

        StepVerifier.create(handler.handle(session)).verifyComplete();

        assertThat(subscriptions.getSessions(userId)).isEmpty();
    }

    @Test
    void continuesAfterMalformedUnsupportedAndInvalidPayloadMessages() {
        when(serialization.deserialize(new byte[]{1})).thenThrow(new IllegalArgumentException("invalid bytes"));
        when(serialization.deserialize(new byte[]{2})).thenReturn(command(MilkCommandType.PING));
        when(serialization.deserialize(new byte[]{3})).thenReturn(
                new UserWebSocketCommand(MilkCommandType.SUBSCRIBE, MilkCommandType.PING));
        when(serialization.deserialize(new byte[]{4})).thenReturn(command(MilkCommandType.SUBSCRIBE));
        when(session.receive()).thenReturn(Flux.just(message(1), message(2), message(3), message(4)));

        StepVerifier.create(handler.handle(session)).verifyComplete();

        assertThat(subscriptions.getSessions(userId)).containsExactly(session);
    }

    @Test
    void ignoresObjectsThatAreNotCommands() {
        when(serialization.deserialize(new byte[]{1})).thenReturn("not a command");
        when(session.receive()).thenReturn(Flux.just(message(1)));

        StepVerifier.create(handler.handle(session)).verifyComplete();

        assertThat(subscriptions.getSessions(userId)).isEmpty();
    }

    @Test
    void waitsForAsyncDispatcherBeforeProcessingNextCommand() {
        WebSocketCommandDispatchService dispatcher = mock(WebSocketCommandDispatchService.class);
        UserWebSocketCommand subscribe = command(MilkCommandType.SUBSCRIBE);
        UserWebSocketCommand unsubscribe = command(MilkCommandType.UNSUBSCRIBE);
        when(serialization.deserialize(new byte[]{1})).thenReturn(subscribe);
        when(serialization.deserialize(new byte[]{2})).thenReturn(unsubscribe);
        when(session.receive()).thenReturn(Flux.just(message(1), message(2)));
        when(dispatcher.dispatch(subscribe, session)).thenAnswer(invocation ->
                Mono.delay(Duration.ofSeconds(1)).doOnNext(ignored -> subscriptions.subscribe(userId, session)).then());
        when(dispatcher.dispatch(unsubscribe, session)).thenAnswer(invocation ->
                Mono.fromRunnable(() -> subscriptions.unsubscribe(userId, session)));
        handler = new UserWebSocketHandler("/router", serialization, dispatcher, subscriptions);

        StepVerifier.withVirtualTime(() -> handler.handle(session))
                .thenAwait(Duration.ofSeconds(1)).verifyComplete();

        assertThat(subscriptions.getSessions(userId)).isEmpty();
        verify(dispatcher).dispatch(subscribe, session);
        verify(dispatcher).dispatch(unsubscribe, session);
    }

    @Test
    void propagatesTransportErrors() {
        IllegalStateException failure = new IllegalStateException("connection failed");
        when(session.receive()).thenReturn(Flux.error(failure));

        StepVerifier.create(handler.handle(session)).expectErrorMatches(error -> error == failure).verify();
    }

    private UserWebSocketCommand command(MilkCommandType type) {
        return new UserWebSocketCommand(type, new MilkSubDTO(UserSubscriptions.createSubscription(userId), type));
    }

    private WebSocketMessage message(int value) {
        return new WebSocketMessage(WebSocketMessage.Type.BINARY,
                DefaultDataBufferFactory.sharedInstance.wrap(new byte[]{(byte) value}));
    }
}
