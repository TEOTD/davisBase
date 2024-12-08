package com.neodymium.davisbase.models.table;

import com.neodymium.davisbase.constants.enums.PageTypes;
import com.neodymium.davisbase.models.Cell;
import com.neodymium.davisbase.models.CellHeader;
import com.neodymium.davisbase.models.CellPayload;

import java.nio.ByteBuffer;

public record TableCell(CellHeader cellHeader, CellPayload cellPayload) implements Cell {
    public static Cell deserialize(byte[] data, PageTypes pageType) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        byte[] headerBytes = new byte[CellHeader.getHeaderSize()];
        buffer.get(headerBytes);
        CellHeader cellHeader;
        if (PageTypes.LEAF.equals(pageType)) {
            cellHeader = TableLeafCellHeader.deserialize(headerBytes);
        } else {
            cellHeader = TableInteriorCellHeader.deserialize(headerBytes);
        }
        byte[] payloadBytes = new byte[buffer.remaining()];
        buffer.get(payloadBytes);
        CellPayload cellPayload = TableCellPayload.deserialize(payloadBytes);
        return new TableCell(cellHeader, cellPayload);
    }

    public static Cell createParentCell(short leftChildPage, int rowId) {
        CellHeader cellHeader = new TableInteriorCellHeader(leftChildPage, rowId);
        return new TableCell(cellHeader, null);
    }

    @Override
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(CellHeader.getHeaderSize() + cellPayload.getSize());
        buffer.put(cellHeader.serialize());
        buffer.put(cellPayload.serialize());
        return buffer.array();
    }

    @Override
    public void delete() {
        cellPayload.delete();
    }

    @Override
    public boolean exists() {
        return cellPayload.exists();
    }
}