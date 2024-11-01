package io;

import java.nio.ByteBuffer;

public class LongSerializer implements FixedSizeSerializer<Long> {

    @Override
    public byte[] serialize(Long value, ByteBuffer buffer) {
        buffer.putLong(value);
        return null;
    }

    @Override
    public Long deserialize(ByteBuffer buffer) {
        return buffer.getLong();
    }

    @Override
    public int getSerializedSize() {
        return 8;
    }
}
