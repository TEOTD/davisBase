package com.neodymium.davisbase.models;

import com.neodymium.davisbase.constants.enums.PageTypes;

import java.nio.ByteBuffer;

public class TableCellFactory implements CellFactory {

    @Override
    public Cell deserialize(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);

        // Extract cell metadata
        int size = buffer.getInt(); // Size of the cell
        int rowId = buffer.getInt(); // Row ID
        byte pageType = buffer.get(); // Page type
        short pageNumber = buffer.getShort(); // Page number

        // Extract serialized record
        byte[] recordData = new byte[data.length - buffer.position()];
        buffer.get(recordData);

        // Deserialize the record
        TableRecord record = TableRecord.fromCell(new TableCell(size, rowId, PageTypes.get(pageType), pageNumber, null, null));

        // Return the fully constructed TableCell
        return new TableCell(size, rowId, PageTypes.get(pageType), pageNumber, record, String.valueOf(rowId));
    }

    @Override
    public Cell createParentCell(int rowId, int pageNo) {
        return new TableCell(
                0, // Size will be set later
                rowId,
                PageTypes.INTERIOR, // Interior page type
                (short) pageNo,
                null, // No associated record for parent cells
                String.valueOf(rowId)
        );
    }
}
