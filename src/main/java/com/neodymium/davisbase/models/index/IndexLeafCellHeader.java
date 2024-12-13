package com.neodymium.davisbase.models.index;
import com.neodymium.davisbase.models.index.CellHeader;

import java.nio.ByteBuffer;

public record IndexLeafCellHeader(short size) implements CellHeader {
    static CellHeader deserialize(byte[] headerBytes){
        if (headerBytes == null || headerBytes.length != Short.BYTES){
            throw new IllegalArgumentException("Invalid headerBytes: Expected length of "+(Short.BYTES+ Integer.BYTES))

        }
        ByteBuffer buffer = ByteBuffer.wrap(headerBytes);
        short size = buffer.getShort();
        return new IndexLeafCellHeader(size);
    }

    @Override
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(Short.BYTES);
        buffer.putShort(size);
        return buffer.array();
    }

    @Override
    public short leftChildPage() {
        return -1;
    }
}
