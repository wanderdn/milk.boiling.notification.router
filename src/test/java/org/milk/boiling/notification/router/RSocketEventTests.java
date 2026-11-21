package org.milk.boiling.notification.router;

import io.rsocket.core.RSocketServer;
import io.rsocket.transport.netty.server.TcpServerTransport;
import org.junit.jupiter.api.Test;
import org.milk.boiling.notification.dto.*;
import org.milk.boiling.notification.dto.websocket.UserWebSocketCommand;
import org.milk.boiling.notification.router.rsocket.RSocketEventController;
import org.milk.boiling.notification.router.loadtest.RSocketLoadTestIngressController;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.annotation.support.RSocketMessageHandler;
import org.springframework.util.MimeTypeUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

class RSocketEventTests {
    private final SerializationService serialization = new SerializationService();
    private final UUID user = UUID.randomUUID();

    private byte[] command(MilkCommandType type) {
        return serialization.serialize(new UserWebSocketCommand(type,
                MilkSubDTO.subMilkSubDTO(UserSubscriptions.createSubscription(user))));
    }

    @Test
    void boundedQueueHonorsDemandAndCleansUpOnCancel() {
        var controller = new RSocketEventController(serialization, 2);
        var commands = Sinks.many().unicast().onBackpressureBuffer();
        var first = new MilkBoilingEvent(UUID.randomUUID(), user, "first");
        var second = new MilkBoilingEvent(UUID.randomUUID(), user, "second");
        StepVerifier.create(controller.events(commands.asFlux().cast(byte[].class)), 0)
                .then(() -> {
                    commands.tryEmitNext(command(MilkCommandType.SUBSCRIBE)).orThrow();
                    commands.tryEmitNext(command(MilkCommandType.SUBSCRIBE)).orThrow();
                    assertThat(controller.offer(new MilkBoilingEvent(UUID.randomUUID(), UUID.randomUUID(), "other"))).isFalse();
                    assertThat(controller.offer(first)).isTrue();
                    assertThat(controller.offer(second)).isTrue();
                    assertThat(controller.offer(first)).isFalse();
                })
                .thenRequest(1)
                .assertNext(bytes -> assertThat(serialization.<UserWebSocketCommand>deserialize(bytes).payload()).isEqualTo(first))
                .thenRequest(1)
                .assertNext(bytes -> assertThat(serialization.<UserWebSocketCommand>deserialize(bytes).payload()).isEqualTo(second))
                .thenCancel().verify(Duration.ofSeconds(5));
        assertThat(controller.offer(first)).isFalse();
    }

    @Test
    void unsubscribeAndInputCompletionReleaseSubscriptions() {
        var controller = new RSocketEventController(serialization, 2);
        var commands = Sinks.many().unicast().<byte[]>onBackpressureBuffer();
        var event = new MilkBoilingEvent(UUID.randomUUID(), user, "event");
        StepVerifier.create(controller.events(commands.asFlux()))
                .then(() -> {
                    commands.tryEmitNext(command(MilkCommandType.SUBSCRIBE)).orThrow();
                    commands.tryEmitNext(command(MilkCommandType.UNSUBSCRIBE)).orThrow();
                    assertThat(controller.offer(event)).isFalse();
                    commands.tryEmitNext(command(MilkCommandType.SUBSCRIBE)).orThrow();
                    commands.tryEmitComplete().orThrow();
                }).verifyComplete();
        assertThat(controller.offer(event)).isFalse();
    }

    @Test
    void cancellingOneChannelPreservesOtherChannelsForSameUser() {
        var controller = new RSocketEventController(serialization, 2);
        var firstEvents = new java.util.ArrayList<UserWebSocketCommand>();
        var secondEvents = new java.util.ArrayList<UserWebSocketCommand>();
        Flux<byte[]> commands = Flux.concat(Flux.just(command(MilkCommandType.SUBSCRIBE)), Flux.never());
        var first = controller.events(commands).subscribe(bytes -> firstEvents.add(serialization.deserialize(bytes)));
        var second = controller.events(commands).subscribe(bytes -> secondEvents.add(serialization.deserialize(bytes)));
        var event = new MilkBoilingEvent(UUID.randomUUID(), user, "shared");
        try {
            assertThat(controller.offer(event)).isTrue();
            assertThat(firstEvents).hasSize(1);
            assertThat(secondEvents).hasSize(1);
            first.dispose();
            assertThat(controller.offer(event)).isTrue();
            assertThat(firstEvents).hasSize(1);
            assertThat(secondEvents).hasSize(2);
        } finally {
            first.dispose();
            second.dispose();
        }
        assertThat(controller.offer(event)).isFalse();
    }

    @Test
    void malformedCommandFailsChannelAndReleasesSubscriptions() {
        var controller = new RSocketEventController(serialization, 2);
        StepVerifier.create(controller.events(Flux.just(command(MilkCommandType.SUBSCRIBE), new byte[]{0})))
                .expectError().verify(Duration.ofSeconds(5));
        assertThat(controller.offer(new MilkBoilingEvent(UUID.randomUUID(), user, "event"))).isFalse();
    }

    @Test
    void requestChannelWorksOverRealTcpWithSpringCodecs() {
        var controller = new RSocketEventController(serialization, 2, 100, Duration.ofMillis(2), "lz4");
        var handler = new RSocketMessageHandler();
        handler.setHandlers(List.of(controller));
        handler.afterPropertiesSet();
        var server = RSocketServer.create(handler.responder())
                .bind(TcpServerTransport.create("127.0.0.1", 0)).block(Duration.ofSeconds(5));
        var requester = RSocketRequester.builder().rsocketStrategies(handler.getRSocketStrategies())
                .dataMimeType(MimeTypeUtils.APPLICATION_OCTET_STREAM).tcp("127.0.0.1", server.address().getPort());
        var commands = Sinks.many().unicast().<byte[]>onBackpressureBuffer();
        var event = new MilkBoilingEvent(UUID.randomUUID(), user, "wire");
        try {
            StepVerifier.create(requester.route("milk.events").data(commands.asFlux(), byte[].class).retrieveFlux(byte[].class))
                    .then(() -> commands.tryEmitNext(command(MilkCommandType.SUBSCRIBE)).orThrow())
                    .then(() -> await().atMost(Duration.ofSeconds(5)).until(() -> controller.offer(event)))
                    .assertNext(bytes -> assertThat(new RSocketBatchCodec(serialization).decode(bytes)).containsExactly(event))
                    .thenCancel().verify(Duration.ofSeconds(10));
            await().atMost(Duration.ofSeconds(5)).until(() -> !controller.offer(event));
        } finally {
            requester.dispose();
            server.dispose();
        }
    }

    @Test
    void fullBatchFlushesImmediatelyAndPartialBatchWaitsForTimeoutWithoutDemand() {
        var controller = new RSocketEventController(serialization, 200, 100, Duration.ofMillis(2), "lz4");
        var commands = Sinks.many().unicast().<byte[]>onBackpressureBuffer();
        var event = new MilkBoilingEvent(UUID.randomUUID(), user, "event");
        var codec = new RSocketBatchCodec(serialization);
        StepVerifier.withVirtualTime(() -> controller.events(commands.asFlux()), 0)
                .then(() -> {
                    commands.tryEmitNext(command(MilkCommandType.SUBSCRIBE)).orThrow();
                    for (int i = 0; i < 100; i++) assertThat(controller.offer(event)).isTrue();
                })
                .thenRequest(1)
                .assertNext(bytes -> assertThat(codec.decode(bytes)).containsExactlyElementsOf(java.util.Collections.nCopies(100, event)))
                .then(() -> assertThat(controller.offer(event)).isTrue())
                .thenAwait(Duration.ofMillis(2))
                .thenRequest(1)
                .assertNext(bytes -> assertThat(codec.decode(bytes)).containsExactly(event))
                .thenCancel().verify(Duration.ofSeconds(5));
        assertThat(controller.stats()).containsEntry("frames", 2L).containsEntry("events", 101L);
    }

    @Test
    void cancelledChannelCannotRegisterAnInFlightDecodedCommand() throws Exception {
        var serializer = mock(SerializationService.class);
        var entered = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        var subscription = new UserWebSocketCommand(MilkCommandType.SUBSCRIBE,
                MilkSubDTO.subMilkSubDTO(UserSubscriptions.createSubscription(user)));
        when(serializer.<UserWebSocketCommand>deserialize(any(byte[].class))).thenAnswer(invocation -> {
            if (calls.incrementAndGet() == 1) {
                entered.countDown();
                if (!release.await(5, java.util.concurrent.TimeUnit.SECONDS)) throw new IllegalStateException("Test timed out");
            }
            return subscription;
        });
        when(serializer.serialize(any())).thenReturn(new byte[]{9});
        var controller = new RSocketEventController(serializer, 2);
        var commands = Sinks.many().unicast().<byte[]>onBackpressureBuffer();
        var first = controller.events(commands.asFlux()).subscribe();
        var emission = java.util.concurrent.CompletableFuture.runAsync(() -> commands.tryEmitNext(new byte[]{1}));
        try {
            assertThat(entered.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            first.dispose();
            release.countDown();
            emission.get(5, java.util.concurrent.TimeUnit.SECONDS);
            var second = controller.events(Flux.concat(Flux.just(new byte[]{2}), Flux.never())).subscribe();
            try {
                // A leaked cancelled channel would make offer fail even though a live subscriber exists.
                assertThat(controller.offer(new MilkBoilingEvent(UUID.randomUUID(), user, "live"))).isTrue();
            } finally {
                second.dispose();
            }
        } finally {
            release.countDown();
            first.dispose();
        }
    }

    @Test
    void ingressReportsPartialAcceptanceAndRejectsOversizeBatches() {
        var controller = new RSocketEventController(serialization, 1);
        var ingress = new RSocketLoadTestIngressController(controller);
        var commands = Sinks.many().unicast().<byte[]>onBackpressureBuffer();
        var event = new MilkBoilingEvent(UUID.randomUUID(), user, "event");
        StepVerifier.create(controller.events(commands.asFlux()), 0).then(() -> {
            commands.tryEmitNext(command(MilkCommandType.SUBSCRIBE)).orThrow();
            var response = ingress.publish(List.of(event, event));
            assertThat(response.getStatusCode().value()).isEqualTo(503);
            assertThat(response.getBody()).isEqualTo(1);
            assertThat(ingress.publish(java.util.Collections.nCopies(5001, event)).getStatusCode().value()).isEqualTo(400);
        }).thenCancel().verify(Duration.ofSeconds(5));
    }
}
