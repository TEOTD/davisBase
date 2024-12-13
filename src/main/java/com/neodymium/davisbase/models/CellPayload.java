package com.neodymium.davisbase.models;

public interface CellPayload {
    byte[] serialize();

    void delete();

    boolean exists();

    Object getKey();

    int getRowId();
}
