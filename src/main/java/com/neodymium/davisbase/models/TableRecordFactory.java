package com.neodymium.davisbase.models;

public interface TableRecordFactory<T> {
    T create(short rowId);

    T deserialize(byte[] data);
}
