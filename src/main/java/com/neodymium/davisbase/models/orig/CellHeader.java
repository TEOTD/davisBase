package com.neodymium.davisbase.models;

public interface CellHeader {
    static int getHeaderSize() {
        return Short.BYTES + Integer.BYTES;
    }

    byte[] serialize();

    short leftChildPage();

    short size();

    int rowId();
}
