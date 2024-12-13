package com.neodymium.davisbase.models.index;
import com.neodymium.davisbase.models.index.CellHeader;

import java.nio.Buffer;
import java.nio.ByteBuffer;

public record IndexInteriorCellHeader(short leftChildPage, short size) implements CellHeader {
    public static CellHeader deserialize(byte[] headerBytes){
        if (headerBytes == null || headerBytes.length != Short.BYTES + Short.BYTES) {
            throw new IllegalArgumentException("Invalid headerBytes: Expected length of "
                    + (Short.BYTES + Short.BYTES));
        }
        ByteBuffer buffer = ByteBuffer.wrap(headerBytes);
        short leftChildPage = buffer.getShort();
        short size = buffer.getShort();
        return new IndexInteriorCellHeader(leftChildPage, size);
    }
    @Override
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(Short.BYTES+Short.BYTES);
        buffer.putShort(leftChildPage);
        buffer.putShort(size);
        return buffer.array();

    }
    @Override
    public short size() {
        return (short) 0;
    }
}
