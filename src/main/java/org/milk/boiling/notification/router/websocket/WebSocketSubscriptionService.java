package org.milk.boiling.notification.router.websocket;

import org.springframework.stereotype.Service;
import org.springframework.web.reactive.socket.WebSocketSession;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class WebSocketSubscriptionService {
    private final Map<UUID, Set<WebSocketSession>> subscriptions = new ConcurrentHashMap<>();

    public void subscribe(UUID userId, WebSocketSession session) {
        subscriptions.compute(userId, (id, sessions) -> {
            if (sessions == null) {
                sessions = ConcurrentHashMap.newKeySet();
            }
            sessions.add(session);
            return sessions;
        });
    }

    public void unsubscribe(UUID userId, WebSocketSession session) {
        subscriptions.computeIfPresent(userId, (id, sessions) -> {
            sessions.remove(session);
            return sessions.isEmpty() ? null : sessions;
        });
    }

    public Set<WebSocketSession> getSessions(UUID userId) {
        Set<WebSocketSession> sessions = subscriptions.get(userId);
        return sessions == null ? Set.of() : Set.copyOf(sessions);
    }

    @Override
    public String toString() {
        return subscriptions.toString();
    }
}
