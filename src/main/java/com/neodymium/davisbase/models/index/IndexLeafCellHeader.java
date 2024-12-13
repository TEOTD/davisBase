package com.neodymium.davisbase.models.index;

import com.neodymium.davisbase.models.CellHeader;

import java.nio.ByteBuffer;

public record IndexLeafCellHeader(byte size) implements CellHeader {
    public static CellHeader deserialize(byte[] headerBytes) {
        if (headerBytes == null || headerBytes.length != getHeaderSize()) {
            throw new IllegalArgumentException("Invalid headerBytes: Expected length of "
                    + getHeaderSize());
        }
        ByteBuffer buffer = ByteBuffer.wrap(headerBytes);
        byte size = buffer.get();
        return new IndexLeafCellHeader(size);
    }

    public static int getHeaderSize() {
        return Byte.BYTES;
    }

    @Override
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(getHeaderSize());
        buffer.put(size);
        return buffer.array();
    }

    @Override
    public short leftChildPage() {
        return 0;
    }

    @Override
    public int rowId() {
        return 0;
    }
}
