package com.neodymium.customdb.models;

public interface TableRecord {
    byte[] toByteArray(int recordSize);

    String getPrimaryKey();
}
