package com.neodymium.davisbase.models.index;

import com.neodymium.davisbase.constants.enums.PageTypes;
import com.neodymium.davisbase.models.Cell;
import com.neodymium.davisbase.models.CellHeader;
import com.neodymium.davisbase.models.CellPayload;
import org.springframework.util.ObjectUtils;

import java.nio.ByteBuffer;

public record IndexCell(CellHeader cellHeader, CellPayload cellPayload) implements Cell {
    public static Cell deserialize(byte[] data, PageTypes pageType) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        CellHeader cellHeader;
        if (PageTypes.LEAF_INDEX.equals(pageType)) {
            byte[] headerBytes = new byte[IndexLeafCellHeader.getHeaderSize()];
            buffer.get(headerBytes);
            cellHeader = IndexLeafCellHeader.deserialize(headerBytes);
        } else {
            byte[] headerBytes = new byte[IndexInteriorCellHeader.getHeaderSize()];
            buffer.get(headerBytes);
            cellHeader = IndexInteriorCellHeader.deserialize(headerBytes);
        }
        byte[] payloadBytes = new byte[buffer.remaining()];
        CellPayload cellPayload = null;
        if (!ObjectUtils.isEmpty(payloadBytes)) {
            buffer.get(payloadBytes);
            cellPayload = IndexCellPayload.deserialize(payloadBytes);
        }
        return new IndexCell(cellHeader, cellPayload);
    }

    public static Cell createParentCell(short leftChildPage, Cell cell) {
        return new IndexCell(new IndexInteriorCellHeader(leftChildPage, cell.cellHeader().size()), cell.cellPayload());
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
    }

    @Override
    public boolean exists() {
        return true;
    }
}
