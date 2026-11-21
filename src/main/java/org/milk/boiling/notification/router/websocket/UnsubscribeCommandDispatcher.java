package org.milk.boiling.notification.router.websocket;

import org.milk.boiling.notification.dto.DTOInterface;
import org.milk.boiling.notification.dto.MilkCommandType;
import org.milk.boiling.notification.dto.MilkSubDTO;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;

@Service
public class UnsubscribeCommandDispatcher implements CommandDispatcher {
    private final WebSocketSubscriptionService subscriptions;

    public UnsubscribeCommandDispatcher(WebSocketSubscriptionService subscriptions) {
        this.subscriptions = subscriptions;
    }

    @Override
    public MilkCommandType getType() {
        return MilkCommandType.UNSUBSCRIBE;
    }

    @Override
    public Mono<Void> process(DTOInterface payload, WebSocketSession session) {
        return Mono.fromRunnable(() -> {
            MilkSubDTO subscription = (MilkSubDTO) payload;
            subscriptions.unsubscribe(subscription.subscriber().getUserId(), session);
        });
    }
}
