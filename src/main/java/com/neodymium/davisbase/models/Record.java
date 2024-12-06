package com.neodymium.davisbase.models;

public interface Record {
    byte[] serialize();

    int getRowId();

    int getSize();

    String getPrimaryKey();

    void delete();

    boolean isDeleted();
}
