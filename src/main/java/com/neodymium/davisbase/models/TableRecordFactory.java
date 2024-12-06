package com.neodymium.davisbase.models;

public interface TableRecordFactory<T extends TableRecord> {
    T deserialize(byte[] data);

    T createParentRecord(int demarcationKey, int pageNo);
}
