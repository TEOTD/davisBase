package com.neodymium.davisbase.models.table;

import com.neodymium.davisbase.models.CellHeader;

import java.nio.ByteBuffer;

public record TableInteriorCellHeader(short leftChildPage, int rowId) implements CellHeader {
    public static CellHeader deserialize(byte[] headerBytes) {
        if (headerBytes == null || headerBytes.length != Short.BYTES + Integer.BYTES) {
            throw new IllegalArgumentException("Invalid headerBytes: Expected length of "
                    + (Short.BYTES + Integer.BYTES));
        }
        ByteBuffer buffer = ByteBuffer.wrap(headerBytes);
        short leftChildPage = buffer.getShort();
        int rowId = buffer.getInt();
        return new TableInteriorCellHeader(leftChildPage, rowId);
    }

    public static int getHeaderSize() {
        return Short.BYTES + Integer.BYTES;
    }

    @Override
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(Short.BYTES + Integer.BYTES);
        buffer.putShort(leftChildPage);
        buffer.putInt(rowId);
        return buffer.array();
    }

    @Override
    public short size() {
        return (short) 0;
    }
}
