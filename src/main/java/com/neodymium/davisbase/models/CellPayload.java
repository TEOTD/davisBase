package com.neodymium.davisbase.models.index;

public interface CellPayload {
    byte[] serialize();

    void delete();

    boolean exists();

    int getSize();

    void getKey();
}
