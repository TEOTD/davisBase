package com.neodymium.davisbase.models;

public interface Cell {
    byte[] serialize();

    int rowId();

    int size();

    short pageNumber();

    String primaryKey();

    void delete();

    boolean exists();
}
