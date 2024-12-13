package com.neodymium.davisbase.models.index;

import com.neodymium.davisbase.models.CellHeader;

import java.nio.ByteBuffer;

public record IndexInteriorCellHeader(short leftChildPage, byte size) implements CellHeader {
    public static CellHeader deserialize(byte[] headerBytes) {
        if (headerBytes == null || headerBytes.length != getHeaderSize()) {
            throw new IllegalArgumentException("Invalid headerBytes: Expected length of "
                    + getHeaderSize());
        }
        ByteBuffer buffer = ByteBuffer.wrap(headerBytes);
        short leftChildPage = buffer.getShort();
        byte size = buffer.get();
        return new IndexInteriorCellHeader(leftChildPage, size);
    }

    public static int getHeaderSize() {
        return Short.BYTES + Byte.BYTES;
    }

    @Override
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(getHeaderSize());
        buffer.putShort(leftChildPage);
        buffer.put(size);
        return buffer.array();
    }

    @Override
    public int rowId() {
        return 0;
    }
}
