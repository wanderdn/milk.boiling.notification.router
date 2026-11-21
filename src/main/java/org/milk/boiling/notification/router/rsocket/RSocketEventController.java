package org.milk.boiling.notification.router.rsocket;

import org.milk.boiling.notification.dto.*;
import org.milk.boiling.notification.dto.websocket.UserWebSocketCommand;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.stereotype.Controller;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.Set;
import java.util.Map;
import java.util.UUID;
import java.time.Duration;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

/** One bounded event queue per request-channel; subscriptions live only as long as that channel. */
@Controller
public class RSocketEventController {
    private final SerializationService serialization;
    private final int capacity;
    private final int batchSize;
    private final Duration batchTimeout;
    private final boolean compress;
    private final RSocketBatchCodec batches;
    private final LongAdder frames = new LongAdder();
    private final LongAdder events = new LongAdder();
    private final LongAdder rawBytes = new LongAdder();
    private final LongAdder wireBytes = new LongAdder();
    private final LongAdder compressedFrames = new LongAdder();
    private final ConcurrentHashMap<UUID, Set<Channel>> subscribers = new ConcurrentHashMap<>();

    public RSocketEventController(SerializationService serialization, int capacity) {
        this(serialization, capacity, 1, Duration.ofMillis(2), "none");
    }

    @Autowired
    public RSocketEventController(SerializationService serialization,
                                 @Value("${app.rsocket.queue-capacity:4096}") int capacity,
                                 @Value("${app.rsocket.batch-size:1}") int batchSize,
                                 @Value("${app.rsocket.batch-timeout:2ms}") Duration batchTimeout,
                                 @Value("${app.rsocket.compression:none}") String compression) {
        if (capacity < 1) throw new IllegalArgumentException("RSocket queue capacity must be positive");
        if (batchSize < 1 || batchSize > RSocketBatchCodec.MAX_EVENTS || batchTimeout.isNegative() || batchTimeout.isZero()) {
            throw new IllegalArgumentException("Invalid RSocket batch settings");
        }
        if (!Set.of("none", "lz4").contains(compression)) throw new IllegalArgumentException("Unknown RSocket compression");
        this.serialization = serialization;
        this.capacity = capacity;
        this.batchSize = batchSize;
        this.batchTimeout = batchTimeout;
        this.compress = compression.equals("lz4");
        this.batches = new RSocketBatchCodec(serialization);
    }

    @MessageMapping("milk.events")
    public Flux<byte[]> events(Flux<byte[]> commands) {
        return Flux.defer(() -> {
            var channel = new Channel(capacity);
            Flux<MilkBoilingEvent> source = channel.events.asFlux()
                    .takeUntilOther(commands.doOnNext(bytes -> apply(channel, serialization.deserialize(bytes))).then());
            Flux<byte[]> encoded = batchSize == 1 && !compress
                    ? source.map(event -> record(new RSocketBatchCodec.Encoded(
                            serialization.serialize(new UserWebSocketCommand(MilkCommandType.PAYLOAD, event)), 0, false), 1))
                    : source.bufferTimeout(batchSize, batchTimeout, true).map(batch -> record(batches.encode(batch, compress), batch.size()));
            return encoded.doFinally(signal -> close(channel));
        });
    }

    private byte[] record(RSocketBatchCodec.Encoded batch, int count) {
        frames.increment();
        events.add(count);
        rawBytes.add(batch.rawBytes() == 0 ? batch.bytes().length : batch.rawBytes());
        wireBytes.add(batch.bytes().length);
        if (batch.compressed()) compressedFrames.increment();
        return batch.bytes();
    }

    public Map<String, Long> stats() {
        return Map.of("frames", frames.sum(), "events", events.sum(), "rawBytes", rawBytes.sum(),
                "wireBytes", wireBytes.sum(), "compressedFrames", compressedFrames.sum());
    }

    private void apply(Channel channel, UserWebSocketCommand command) {
        synchronized (channel) {
            if (!channel.closed) applyOpen(channel, command);
        }
    }

    private void close(Channel channel) {
        synchronized (channel) {
            channel.closed = true;
            channel.users.forEach(user -> unsubscribe(channel, user));
        }
    }

    private void applyOpen(Channel channel, UserWebSocketCommand command) {
        if (command.type() == MilkCommandType.PING) return;
        if (!(command.payload() instanceof MilkSubDTO subscription)) {
            throw new IllegalArgumentException("Expected RSocket subscription command");
        }
        UUID user = subscription.subscriber().getUserId();
        if (command.type() == MilkCommandType.SUBSCRIBE) {
            channel.users.add(user);
            subscribers.compute(user, (id, channels) -> {
                if (channels == null) channels = ConcurrentHashMap.newKeySet();
                channels.add(channel);
                return channels;
            });
        } else if (command.type() == MilkCommandType.UNSUBSCRIBE) {
            unsubscribe(channel, user);
        } else {
            throw new IllegalArgumentException("Unsupported RSocket command: " + command.type());
        }
    }

    private void unsubscribe(Channel channel, UUID user) {
        channel.users.remove(user);
        subscribers.computeIfPresent(user, (id, channels) -> {
            channels.remove(channel);
            return channels.isEmpty() ? null : channels;
        });
    }

    /** False means no subscriber or a full channel queue; callers must account for rejected events. */
    public boolean offer(MilkBoilingEvent event) {
        var channels = subscribers.get(event.userId());
        if (channels == null || channels.isEmpty()) return false;
        for (var channel : channels) {
            synchronized (channel.events) {
                if (channel.events.tryEmitNext(event).isFailure()) return false;
            }
        }
        return true;
    }

    private static class Channel {
        boolean closed;
        final Set<UUID> users = ConcurrentHashMap.newKeySet();
        final Sinks.Many<MilkBoilingEvent> events;

        Channel(int capacity) {
            events = Sinks.many().unicast().onBackpressureBuffer(new ArrayBlockingQueue<MilkBoilingEvent>(capacity));
        }
    }
}
