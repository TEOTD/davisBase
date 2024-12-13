package com.neodymium.davisbase.models.index;

public interface Cell {
    CellHeader cellHeader();

    CellPayload cellPayload();

    byte[] serialize();

    void delete();

    boolean exists();
}
