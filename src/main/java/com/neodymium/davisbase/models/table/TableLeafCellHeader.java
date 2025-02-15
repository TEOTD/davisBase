package com.neodymium.davisbase.models.table;

import com.neodymium.davisbase.models.CellHeader;

import java.nio.ByteBuffer;

public record TableLeafCellHeader(byte size, int rowId) implements CellHeader {
    static CellHeader deserialize(byte[] headerBytes) {
        if (headerBytes == null || headerBytes.length != getHeaderSize()) {
            throw new IllegalArgumentException("Invalid headerBytes: Expected length of "
                    + getHeaderSize());
        }
        ByteBuffer buffer = ByteBuffer.wrap(headerBytes);
        byte size = buffer.get();
        int rowId = buffer.getInt();
        return new TableLeafCellHeader(size, rowId);
    }

    public static int getHeaderSize() {
        return Byte.BYTES + Integer.BYTES;
    }

    @Override
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(getHeaderSize());
        buffer.put(size);
        buffer.putInt(rowId);
        return buffer.array();
    }

    @Override
    public short leftChildPage() {
        return -1;
    }
}