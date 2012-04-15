package com.afewmoreamps;

import java.nio.ByteBuffer;

public class KDEntry {
    public static final int SIZE = 21;
    public final int fileId;
    public final int valueSize;
    public final int valuePos;
    public final long timestamp;
    public final byte flags;

    /*
     * Null KDEntry for return by compareAndSet when the expected value ends up being null
     */
    KDEntry() {
        fileId = -1;
        valueSize = -1;
        valuePos = -1;
        timestamp = -1;
        flags = -1;
    }

    KDEntry(byte contents[]) {
        ByteBuffer buf = ByteBuffer.wrap(contents);
        fileId = buf.getInt();
        valueSize = buf.getInt();
        valuePos = buf.getInt();
        timestamp = buf.getLong();
        flags = buf.get();
    }

    public static void toBytes(ByteBuffer out, int fileId, int valueSize, int valuePos, long timestamp, byte flags) {
        out.clear();
        out.putInt(fileId);
        out.putInt(valueSize);
        out.putInt(valuePos);
        out.putLong(timestamp);
        out.put(flags);
        assert(out.position() == out.capacity());
    }

    public static byte[] toBytes(int fileId, int valueSize, int valuePos, long timestamp, byte flags) {
        ByteBuffer out = ByteBuffer.allocate(SIZE);
        out.putInt(fileId);
        out.putInt(valueSize);
        out.putInt(valuePos);
        out.putLong(timestamp);
        out.put(flags);
        assert(out.position() == out.capacity());
        return out.array();
    }
}