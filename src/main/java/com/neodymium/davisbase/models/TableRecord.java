package com.neodymium.davisbase.models;

public interface TableRecord<T> {
    byte[] serialize();

    String getPrimaryKey();

    T deserialize(byte[] data);
}
