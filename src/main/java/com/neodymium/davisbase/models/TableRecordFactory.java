package com.neodymium.davisbase.models;

public interface TableRecordFactory<T extends Record> {
    T deserialize(byte[] data);

    T createParentRecord(int demarcationKey, int pageNo);
}
