package com.neodymium.davisbase.models.table;

import com.neodymium.davisbase.constants.enums.PageTypes;
import com.neodymium.davisbase.models.Cell;
import com.neodymium.davisbase.models.CellHeader;
import com.neodymium.davisbase.models.CellPayload;
import org.springframework.util.ObjectUtils;

import java.nio.ByteBuffer;

public record TableCell(CellHeader cellHeader, CellPayload cellPayload) implements Cell {
    public static Cell deserialize(byte[] data, PageTypes pageType) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        CellHeader cellHeader;
        if (PageTypes.LEAF.equals(pageType)) {
            byte[] headerBytes = new byte[TableLeafCellHeader.getHeaderSize()];
            buffer.get(headerBytes);
            cellHeader = TableLeafCellHeader.deserialize(headerBytes);
        } else {
            byte[] headerBytes = new byte[TableInteriorCellHeader.getHeaderSize()];
            buffer.get(headerBytes);
            cellHeader = TableInteriorCellHeader.deserialize(headerBytes);
        }
        byte[] payloadBytes = new byte[buffer.remaining()];
        CellPayload cellPayload = null;
        if (!ObjectUtils.isEmpty(payloadBytes)) {
            buffer.get(payloadBytes);
            cellPayload = TableCellPayload.deserialize(payloadBytes);
        }
        return new TableCell(cellHeader, cellPayload);
    }

    public static Cell createParentCell(short leftChildPage, int rowId) {
        CellHeader cellHeader = new TableInteriorCellHeader(leftChildPage, rowId);
        return new TableCell(cellHeader, null);
    }

    @Override
    public byte[] serialize() {
        byte[] cellHeaderBytes = cellHeader.serialize();
        byte[] cellPayloadBytes = new byte[0];
        if (!ObjectUtils.isEmpty(cellPayload)) {
            cellPayloadBytes = cellPayload.serialize();
        }
        ByteBuffer buffer = ByteBuffer.allocate(cellHeaderBytes.length + cellPayloadBytes.length);
        buffer.put(cellHeaderBytes);
        buffer.put(cellPayloadBytes);
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