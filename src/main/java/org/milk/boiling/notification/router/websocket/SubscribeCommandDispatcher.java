package org.milk.boiling.notification.router.websocket;

import org.milk.boiling.notification.dto.DTOInterface;
import org.milk.boiling.notification.dto.MilkCommandType;
import org.milk.boiling.notification.dto.MilkSubDTO;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;

@Service
public class SubscribeCommandDispatcher implements CommandDispatcher {
    private final WebSocketSubscriptionService subscriptions;

    public SubscribeCommandDispatcher(WebSocketSubscriptionService subscriptions) {
        this.subscriptions = subscriptions;
    }

    @Override
    public MilkCommandType getType() {
        return MilkCommandType.SUBSCRIBE;
    }

    @Override
    public Mono<Void> process(DTOInterface payload, WebSocketSession session) {
        return Mono.fromRunnable(() -> {
            MilkSubDTO subscription = (MilkSubDTO) payload;
            subscriptions.subscribe(subscription.subscriber().getUserId(), session);
        });
    }
}
