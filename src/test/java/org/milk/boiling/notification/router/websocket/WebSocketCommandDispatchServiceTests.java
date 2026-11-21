package org.milk.boiling.notification.router.websocket;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.milk.boiling.notification.dto.MilkCommandType;
import org.milk.boiling.notification.dto.MilkSubDTO;
import org.milk.boiling.notification.dto.UserSubscriptions;
import org.milk.boiling.notification.dto.websocket.UserWebSocketCommand;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.Mockito.*;

class WebSocketCommandDispatchServiceTests {
    private final WebSocketSession session = mock(WebSocketSession.class);
    private final UUID userId = UUID.randomUUID();
    private WebSocketSubscriptionService subscriptions;
    private WebSocketCommandDispatchService dispatcher;

    @BeforeEach
    void setUp() {
        subscriptions = new WebSocketSubscriptionService();
        dispatcher = new WebSocketCommandDispatchService(List.of(
                new SubscribeCommandDispatcher(subscriptions), new UnsubscribeCommandDispatcher(subscriptions)));
    }

    @Test
    void subscribesOnlyWhenSubscribedToPublisher() {
        Mono<Void> result = dispatcher.dispatch(command(MilkCommandType.SUBSCRIBE), session);

        assertThat(subscriptions.getSessions(userId)).isEmpty();
        StepVerifier.create(result).verifyComplete();
        assertThat(subscriptions.getSessions(userId)).containsExactly(session);
    }

    @Test
    void unsubscribeRemovesOnlyTheRequestingSession() {
        WebSocketSession otherSession = mock(WebSocketSession.class);
        StepVerifier.create(dispatcher.dispatch(command(MilkCommandType.SUBSCRIBE), session)).verifyComplete();
        StepVerifier.create(dispatcher.dispatch(command(MilkCommandType.SUBSCRIBE), otherSession)).verifyComplete();

        StepVerifier.create(dispatcher.dispatch(command(MilkCommandType.UNSUBSCRIBE), session)).verifyComplete();

        assertThat(subscriptions.getSessions(userId)).containsExactly(otherSession);
    }

    @ParameterizedTest
    @EnumSource(value = MilkCommandType.class, names = {"SUBSCRIBE", "UNSUBSCRIBE"}, mode = EnumSource.Mode.EXCLUDE)
    void unsupportedCommandProducesReactiveError(MilkCommandType type) {
        StepVerifier.create(dispatcher.dispatch(command(type), session))
                .expectErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining(type.name()))
                .verify();
        assertThat(subscriptions.getSessions(userId)).isEmpty();
    }

    @ParameterizedTest
    @EnumSource(value = MilkCommandType.class, names = {"SUBSCRIBE", "UNSUBSCRIBE"})
    void wrongPayloadProducesReactiveError(MilkCommandType type) {
        StepVerifier.create(dispatcher.dispatch(new UserWebSocketCommand(type, MilkCommandType.PING), session))
                .expectError(ClassCastException.class).verify();
        assertThat(subscriptions.getSessions(userId)).isEmpty();
    }

    @Test
    void propagatesHandlerFailure() {
        CommandDispatcher handler = mock(CommandDispatcher.class);
        when(handler.getType()).thenReturn(MilkCommandType.SUBSCRIBE);
        UserWebSocketCommand command = command(MilkCommandType.SUBSCRIBE);
        RuntimeException failure = new IllegalStateException("handler failed");
        when(handler.process(command.payload(), session)).thenReturn(Mono.error(failure));
        WebSocketCommandDispatchService service = new WebSocketCommandDispatchService(List.of(handler));

        StepVerifier.create(service.dispatch(command, session)).expectErrorMatches(error -> error == failure).verify();
    }

    @Test
    void rejectsDuplicateCommandHandlers() {
        assertThatIllegalStateException().isThrownBy(() -> new WebSocketCommandDispatchService(List.of(
                new SubscribeCommandDispatcher(subscriptions), new SubscribeCommandDispatcher(subscriptions))));
    }

    private UserWebSocketCommand command(MilkCommandType type) {
        return new UserWebSocketCommand(type, new MilkSubDTO(UserSubscriptions.createSubscription(userId), type));
    }
}
