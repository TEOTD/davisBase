package com.neodymium.davisbase.models.table;

import com.neodymium.davisbase.constants.enums.DataTypes;
import com.neodymium.davisbase.constants.enums.PageTypes;
import com.neodymium.davisbase.models.Cell;
import com.neodymium.davisbase.models.CellHeader;
import com.neodymium.davisbase.models.CellPayload;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public record Row(int id, Map<Column, Object> data) {
    private static int estimateSize(Column column) {
        DataTypes dataTypes = DataTypes.getFromTypeCode(column.typeCode());
        if (DataTypes.TEXT.equals(dataTypes)) {
            return column.typeCode() - 0x0C;
        }
        return dataTypes.getSize();
    }

    public static Row fromCell(Cell cell, List<Column> columns) {
        Map<Column, Object> rowData = new HashMap<>();
        ByteBuffer buffer = ByteBuffer.wrap(cell.cellPayload().serialize());
        for (Column column : columns) {
            switch (column.typeCode()) {
                case 0x00 -> rowData.put(column, null);
                case 0x01, 0x08 -> rowData.put(column, buffer.get());
                case 0x02 -> rowData.put(column, buffer.getShort());
                case 0x03, 0x09 -> rowData.put(column, buffer.getInt());
                case 0x04, 0x0A, 0x0B -> rowData.put(column, buffer.getLong());
                case 0x05 -> rowData.put(column, buffer.getFloat());
                case 0x06 -> rowData.put(column, buffer.getDouble());
                default -> {
                    int size = column.typeCode() - 0x0C;
                    byte[] textBytes = new byte[size];
                    buffer.get(textBytes);
                    rowData.put(column, new String(textBytes));
                }
            }
        }
        return new Row(cell.cellHeader().rowId(), rowData);
    }

    public Cell cellFromRow() {
        int estimatedSize = data.keySet().stream()
                .mapToInt(Row::estimateSize)
                .sum();

        ByteBuffer bodyBuffer = ByteBuffer.allocate(estimatedSize);
        byte[] typeCodes = new byte[data.size()];
        int index = 0;
        for (Map.Entry<Column, Object> entry : data.entrySet()) {
            Column column = entry.getKey();
            Object value = entry.getValue();

            if (value == null) {
                typeCodes[index++] = 0x00;
                continue;
            }
            typeCodes[index++] = column.typeCode();
            switch (column.typeCode()) {
                case 0x01, 0x08 -> bodyBuffer.put(((Number) value).byteValue());
                case 0x02 -> bodyBuffer.putShort(((Number) value).shortValue());
                case 0x03, 0x09 -> bodyBuffer.putInt(((Number) value).intValue());
                case 0x04, 0x0A, 0x0B -> bodyBuffer.putLong(((Number) value).longValue());
                case 0x05 -> bodyBuffer.putFloat(((Number) value).floatValue());
                case 0x06 -> bodyBuffer.putDouble(((Number) value).doubleValue());
                default -> {
                    String strValue = (String) value;
                    byte[] textBytes = strValue.getBytes();
                    bodyBuffer.put(textBytes);
                }
            }
        }

        byte[] body = bodyBuffer.array();

        CellPayload cellPayload = new TableCellPayload((byte) 0, (byte) data.size(), typeCodes, body);
        CellHeader cellHeader = new TableLeafCellHeader((byte) cellPayload.serialize().length, id);

        ByteBuffer payloadBuffer = ByteBuffer.allocate(cellHeader.serialize().length + cellPayload.serialize().length);
        payloadBuffer.put(cellHeader.serialize());
        payloadBuffer.put(cellPayload.serialize());
        byte[] payload = payloadBuffer.array();
        return TableCell.deserialize(payload, PageTypes.LEAF);
    }
}
