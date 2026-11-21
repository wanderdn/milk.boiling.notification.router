package org.milk.boiling.notification.router;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.milk.boiling.notification.dto.MilkBoilingEvent;
import org.milk.boiling.notification.dto.MilkCommandType;
import org.milk.boiling.notification.dto.SerializationService;
import org.milk.boiling.notification.dto.websocket.UserWebSocketCommand;
import org.milk.boiling.notification.router.websocket.WebSocketCommandDispatchService;
import org.milk.boiling.notification.router.websocket.WebSocketSubscriptionService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.UUID;

@Component
@org.springframework.context.annotation.Profile("!load-test")
@Slf4j

public class UserWebSocketHandler implements WebSocketHandler {
    @Getter
    private final String path;
    private final DataBufferFactory factory;
    private final SerializationService serializationService;
    private final WebSocketCommandDispatchService commandDispatchService;
    private final WebSocketSubscriptionService subscriptions;

    public UserWebSocketHandler(@Value("${app.boiling.notification.websocket.path:/router}") String path,
                               SerializationService serializationService,
                               WebSocketCommandDispatchService commandDispatchService,
                               WebSocketSubscriptionService subscriptions) {
        this.path = path;
        this.serializationService = serializationService;
        factory = new DefaultDataBufferFactory();
        this.commandDispatchService = commandDispatchService;
        this.subscriptions = subscriptions;
    }


    @Override
    public Mono<Void> handle(WebSocketSession session) {
        return session.send(Flux.just(new WebSocketMessage(WebSocketMessage.Type.BINARY, factory.wrap(serializationService.serialize(new UserWebSocketCommand(MilkCommandType.PAYLOAD, new MilkBoilingEvent(UUID.randomUUID(), UUID.randomUUID(), "asd")))))).repeat()
                .delayElements(Duration.ofSeconds(1))
        ).and(session.receive().map(webSocketMessage -> {
                    byte[] bytes = new byte[webSocketMessage.getPayload().readableByteCount()];
                    webSocketMessage.getPayload().read(bytes);
                    return bytes;
                })
                .concatMap(bytes -> Mono.fromCallable(() -> serializationService.<Object>deserialize(bytes))
                        .ofType(UserWebSocketCommand.class)
                        .flatMap(command -> commandDispatchService.dispatch(command, session))
                        .onErrorResume(error -> {
                            log.warn("Failed to process websocket command", error);
                            return Mono.empty();
                        })));
    }


    @Scheduled(fixedRate = 1000L)
    public Mono<Void> send() {
        return Mono.fromRunnable(() -> log.info("Sending user websocket commands : {}", subscriptions));
    }
}
