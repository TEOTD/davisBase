package com.neodymium.davisbase.models;

public interface TableCellFactory {
    Cell deserialize(byte[] data);

    Cell createParentRecord(int demarcationKey, int pageNo);
}