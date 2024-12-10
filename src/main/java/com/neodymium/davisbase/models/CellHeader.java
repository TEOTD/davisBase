package com.neodymium.davisbase.models;

public interface CellHeader {
    byte[] serialize();

    short leftChildPage();

    byte size();

    int rowId();
}
