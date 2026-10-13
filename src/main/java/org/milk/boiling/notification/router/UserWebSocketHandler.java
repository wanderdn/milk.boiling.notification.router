package org.milk.boiling.notification.router;

import lombok.extern.slf4j.Slf4j;
import org.milk.boiling.notification.dto.MilkBoilingEvent;
import org.milk.boiling.notification.dto.MilkCommandType;
import org.milk.boiling.notification.dto.MilkSubDTO;
import org.milk.boiling.notification.dto.SerializationService;
import org.milk.boiling.notification.dto.websocket.UserWebSocketCommand;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Slf4j

public class UserWebSocketHandler implements WebSocketHandler {

    private final String path;
    private final DataBufferFactory factory;
    private final SerializationService serializationService;
    private final Map<UUID, Set<WebSocketSession>> userWebSocketCommands;

    public UserWebSocketHandler(@Value("${app.boiling.notification.websocket.path:/router}") String path, SerializationService serializationService) {
        this.path = path;
        this.serializationService = serializationService;
        factory = new DefaultDataBufferFactory();
        userWebSocketCommands = new ConcurrentHashMap<>();
    }

    public String getPath() {
        return path;
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
                .map(serializationService::deserialize)
                .doOnNext(object -> {
                    if (object instanceof UserWebSocketCommand(
                            MilkCommandType z, _
                    )
                    ) switch (z) {
                        case SUBSCRIBE -> {
                            userWebSocketCommands.compute(((MilkSubDTO) ((UserWebSocketCommand) object).getPayload()).subscriber().getUserId(), (_, sessionSet) -> {
                                sessionSet = sessionSet != null ? sessionSet : new HashSet<>();
                                sessionSet.add(session);
                                return sessionSet;
                            });
                            log.error(userWebSocketCommands.toString());
                        }
                        case UNSUBSCRIBE ->
                                userWebSocketCommands.compute(((MilkSubDTO) ((UserWebSocketCommand) object).getPayload()).subscriber().getUserId(), (_, sessionSet) -> {
                                    Optional.ofNullable(sessionSet).ifPresent(x -> x.remove(session));
                                    return sessionSet;
                                });
                    }

                })).onErrorContinue((x, y) -> log.error(x.getMessage()));
    }


    @Scheduled(fixedRate = 1000L)
    public Mono<Void> send() {
        return Mono.fromRunnable(() -> log.info("Sending user websocket commands : {}", userWebSocketCommands));
    }
}
