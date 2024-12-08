package com.neodymium.davisbase.models.table;

import com.neodymium.davisbase.models.CellHeader;

import java.nio.ByteBuffer;

public record TableLeafCellHeader(short size, int rowId) implements CellHeader {
    static CellHeader deserialize(byte[] headerBytes) {
        if (headerBytes == null || headerBytes.length != Short.BYTES + Integer.BYTES) {
            throw new IllegalArgumentException("Invalid headerBytes: Expected length of "
                    + (Short.BYTES + Integer.BYTES));
        }
        ByteBuffer buffer = ByteBuffer.wrap(headerBytes);
        short size = buffer.getShort();
        int rowId = buffer.getInt();
        return new TableLeafCellHeader(size, rowId);
    }

    @Override
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(Short.BYTES + Integer.BYTES);
        buffer.putShort(size);
        buffer.putInt(rowId);
        return buffer.array();
    }

    @Override
    public short leftChildPage() {
        return -1;
    }
}