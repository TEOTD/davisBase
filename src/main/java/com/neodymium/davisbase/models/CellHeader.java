package com.neodymium.davisbase.models;

public interface CellHeader {
    byte[] serialize();

    short leftChildPage();

    short size();

    int rowId();
}
