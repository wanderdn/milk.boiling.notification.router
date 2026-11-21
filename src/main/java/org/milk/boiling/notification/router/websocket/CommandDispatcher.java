package org.milk.boiling.notification.router.websocket;

import org.milk.boiling.notification.dto.DTOInterface;
import org.milk.boiling.notification.dto.MilkCommandType;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;

public interface CommandDispatcher {
    MilkCommandType getType();

    Mono<Void> process(DTOInterface payload, WebSocketSession session);
}
