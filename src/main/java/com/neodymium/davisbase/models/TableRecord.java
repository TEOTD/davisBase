package com.neodymium.davisbase.models;

public interface TableRecord {
    byte[] serialize();

    String getPrimaryKey();
}
