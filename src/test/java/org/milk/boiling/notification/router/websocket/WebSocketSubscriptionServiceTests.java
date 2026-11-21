package org.milk.boiling.notification.router.websocket;

import org.junit.jupiter.api.Test;
import org.springframework.web.reactive.socket.WebSocketSession;

import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;

class WebSocketSubscriptionServiceTests {
    private final WebSocketSubscriptionService subscriptions = new WebSocketSubscriptionService();
    private final WebSocketSession session = mock(WebSocketSession.class);
    private final UUID userId = UUID.randomUUID();

    @Test
    void repeatedSubscribeIsIdempotent() {
        subscriptions.subscribe(userId, session);
        subscriptions.subscribe(userId, session);

        assertThat(subscriptions.getSessions(userId)).containsExactly(session);
    }

    @Test
    void usersHaveIndependentSubscriptions() {
        UUID otherUserId = UUID.randomUUID();
        subscriptions.subscribe(userId, session);
        subscriptions.subscribe(otherUserId, session);

        subscriptions.unsubscribe(userId, session);

        assertThat(subscriptions.getSessions(userId)).isEmpty();
        assertThat(subscriptions.getSessions(otherUserId)).containsExactly(session);
    }

    @Test
    void unsubscribeMissingUserOrSessionIsNoOp() {
        subscriptions.unsubscribe(userId, session);
        subscriptions.subscribe(userId, session);
        subscriptions.unsubscribe(userId, mock(WebSocketSession.class));

        assertThat(subscriptions.getSessions(userId)).containsExactly(session);
    }

    @Test
    void canResubscribeAfterLastSessionIsRemoved() {
        subscriptions.subscribe(userId, session);
        subscriptions.unsubscribe(userId, session);
        assertThat(subscriptions.getSessions(userId)).isEmpty();

        subscriptions.subscribe(userId, session);

        assertThat(subscriptions.getSessions(userId)).containsExactly(session);
    }

    @Test
    void sessionsAreAnImmutableSnapshot() {
        subscriptions.subscribe(userId, session);
        Set<WebSocketSession> snapshot = subscriptions.getSessions(userId);
        subscriptions.unsubscribe(userId, session);

        assertThat(snapshot).containsExactly(session);
        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(snapshot::clear);
    }
}
