package com.neodymium.davisbase.models;

import com.neodymium.davisbase.constants.enums.PageTypes;

public record TableCell(int size, int rowId, PageTypes pageType, short pageNumber, Record data,
                        String primaryKey) implements Cell {

    @Override
    public byte[] serialize() {
        return new byte[0];
    }

    @Override
    public void delete() {
        data.delete();
    }

    @Override
    public boolean exists() {
        return data.exists();
    }
}