package com.neodymium.davisbase.models;

public interface CellFactory {
    Cell deserialize(byte[] data);

    Cell createParentCell(int rowId, int pageNo);
}