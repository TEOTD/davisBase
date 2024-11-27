package com.neodymium.davisbase.models;

public interface TableRecord {
    byte[] toByteArray(int recordSize);

    String getPrimaryKey();
}
