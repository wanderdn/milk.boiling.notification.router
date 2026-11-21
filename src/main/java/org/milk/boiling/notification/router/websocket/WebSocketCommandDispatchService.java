package org.milk.boiling.notification.router.websocket;

import org.milk.boiling.notification.dto.MilkCommandType;
import org.milk.boiling.notification.dto.websocket.UserWebSocketCommand;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class WebSocketCommandDispatchService {
    private final Map<MilkCommandType, CommandDispatcher> dispatchers;

    public WebSocketCommandDispatchService(List<CommandDispatcher> dispatchers) {
        this.dispatchers = dispatchers.stream()
                .collect(Collectors.toUnmodifiableMap(CommandDispatcher::getType, Function.identity()));
    }

    public Mono<Void> dispatch(UserWebSocketCommand command, WebSocketSession session) {
        return Mono.defer(() -> {
            CommandDispatcher dispatcher = dispatchers.get(command.type());
            if (dispatcher == null) {
                return Mono.error(new IllegalArgumentException("Dispatcher not found for command type " + command.type()));
            }
            return dispatcher.process(command.payload(), session);
        });
    }
}
