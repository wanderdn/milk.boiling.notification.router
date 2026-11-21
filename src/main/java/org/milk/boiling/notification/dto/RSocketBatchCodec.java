package org.milk.boiling.notification.dto;

import net.jpountz.lz4.LZ4Factory;
import org.milk.boiling.notification.dto.websocket.UserWebSocketCommand;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

/** Versioned binary batch envelope, shared verbatim by router and emitter. */
public class RSocketBatchCodec {
    private static final int MAGIC = 0x4d424231; // MBB1
    private static final int HEADER = 9;
    public static final int MAX_RAW_BYTES = 128 * 1024;
    public static final int MAX_EVENTS = 1000;
    private static final LZ4Factory LZ4 = LZ4Factory.fastestInstance();
    private final SerializationService serialization;

    public RSocketBatchCodec(SerializationService serialization) {
        this.serialization = serialization;
    }

    public record Encoded(byte[] bytes, int rawBytes, boolean compressed) {}

    public Encoded encode(List<MilkBoilingEvent> events, boolean compress) {
        if (events.isEmpty() || events.size() > MAX_EVENTS) {
            throw new IllegalArgumentException("Invalid RSocket batch count");
        }
        byte[] raw = serialization.serialize(events.toArray(MilkBoilingEvent[]::new));
        if (raw.length > MAX_RAW_BYTES) throw new IllegalArgumentException("RSocket batch is too large");
        byte[] body = raw;
        boolean compressed = false;
        if (compress) {
            byte[] candidate = LZ4.fastCompressor().compress(raw);
            if (candidate.length < raw.length) {
                body = candidate;
                compressed = true;
            }
        }
        byte[] frame = ByteBuffer.allocate(HEADER + body.length).putInt(MAGIC)
                .put((byte) (compressed ? 1 : 0)).putInt(raw.length).put(body).array();
        return new Encoded(frame, raw.length, compressed);
    }

    public List<MilkBoilingEvent> decode(byte[] frame) {
        if (frame.length > MAX_RAW_BYTES + HEADER) throw new IllegalArgumentException("RSocket frame is too large");
        if (frame.length < 4 || ByteBuffer.wrap(frame).getInt() != MAGIC) {
            UserWebSocketCommand command = serialization.deserialize(frame);
            if (command.type() != MilkCommandType.PAYLOAD || !(command.payload() instanceof MilkBoilingEvent event)) {
                throw new IllegalArgumentException("Unexpected RSocket event");
            }
            return List.of(event);
        }
        if (frame.length <= HEADER) throw new IllegalArgumentException("Truncated RSocket batch");
        var header = ByteBuffer.wrap(frame);
        header.getInt();
        int flags = Byte.toUnsignedInt(header.get());
        int length = header.getInt();
        if (flags > 1 || length < 1 || length > MAX_RAW_BYTES) {
            throw new IllegalArgumentException("Invalid RSocket batch header");
        }
        byte[] raw;
        if (flags == 1) {
            raw = new byte[length];
            int restored = LZ4.safeDecompressor().decompress(frame, HEADER, frame.length - HEADER, raw, 0, length);
            if (restored != length) throw new IllegalArgumentException("RSocket batch length mismatch");
        } else {
            if (frame.length - HEADER != length) throw new IllegalArgumentException("RSocket batch length mismatch");
            raw = Arrays.copyOfRange(frame, HEADER, frame.length);
        }
        MilkBoilingEvent[] events = serialization.deserialize(raw);
        if (events.length == 0 || events.length > MAX_EVENTS || Arrays.stream(events).anyMatch(java.util.Objects::isNull)) {
            throw new IllegalArgumentException("Invalid RSocket batch contents");
        }
        return Arrays.asList(events);
    }
}
