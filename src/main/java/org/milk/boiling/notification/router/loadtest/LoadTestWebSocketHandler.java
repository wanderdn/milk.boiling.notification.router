package org.milk.boiling.notification.router.loadtest;

import org.milk.boiling.notification.dto.*;
import org.milk.boiling.notification.dto.websocket.UserWebSocketCommand;
import org.milk.boiling.notification.router.UserWebSocketHandler;
import org.milk.boiling.notification.router.websocket.*;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.*;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

/** Controlled event source for transport benchmarks; never active in the normal profile. */
@Component
@Profile("load-test")
public class LoadTestWebSocketHandler extends UserWebSocketHandler {
    private final SerializationService serialization;
    private final WebSocketCommandDispatchService dispatcher;
    private final WebSocketSubscriptionService subscriptions;
    private final ConcurrentHashMap<WebSocketSession, Sinks.Many<MilkBoilingEvent>> outgoing = new ConcurrentHashMap<>();

    public LoadTestWebSocketHandler(SerializationService serialization, WebSocketCommandDispatchService dispatcher,
                                   WebSocketSubscriptionService subscriptions) {
        super("/router", serialization, dispatcher, subscriptions);
        this.serialization = serialization;
        this.dispatcher = dispatcher;
        this.subscriptions = subscriptions;
    }

    @Override
    public Mono<Void> handle(WebSocketSession session) {
        Sinks.Many<MilkBoilingEvent> queue = Sinks.many().unicast()
                .onBackpressureBuffer(new ArrayBlockingQueue<MilkBoilingEvent>(4096));
        outgoing.put(session, queue);
        Set<UUID> users = ConcurrentHashMap.newKeySet();
        Mono<Void> send = session.send(queue.asFlux().map(event -> session.binaryMessage(factory ->
                factory.wrap(serialization.serialize(new UserWebSocketCommand(MilkCommandType.PAYLOAD, event))))));
        Mono<Void> receive = session.receive().map(message -> {
            byte[] bytes = new byte[message.getPayload().readableByteCount()];
            message.getPayload().read(bytes);
            return bytes;
        }).concatMap(bytes -> Mono.fromCallable(() -> serialization.<UserWebSocketCommand>deserialize(bytes))
                .flatMap(command -> dispatcher.dispatch(command, session).doOnSuccess(ignored -> {
                    if (command.payload() instanceof MilkSubDTO subscription) {
                        users.add(subscription.subscriber().getUserId());
                    }
                }))).then();
        return Mono.firstWithSignal(send, receive).doFinally(signal -> {
            outgoing.remove(session);
            users.forEach(user -> subscriptions.unsubscribe(user, session));
        });
    }

    public boolean offer(MilkBoilingEvent event) {
        var sessions = subscriptions.getSessions(event.userId());
        if (sessions.isEmpty()) {
            return false;
        }
        for (var session : sessions) {
            var queue = outgoing.get(session);
            if (queue == null) {
                return false;
            }
            synchronized (queue) {
                if (queue.tryEmitNext(event).isFailure()) {
                    return false;
                }
            }
        }
        return true;
    }
}
