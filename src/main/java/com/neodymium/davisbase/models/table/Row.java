package com.neodymium.davisbase.models.table;

import com.neodymium.davisbase.constants.enums.DataTypes;
import com.neodymium.davisbase.constants.enums.PageTypes;
import com.neodymium.davisbase.models.Cell;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public record Row(Map<Column, Object> data) {
    private static int estimateSize(Column column, Object value) {
        return DataTypes.TEXT.equals(column.dataType()) ? String.valueOf(value).length() : column.dataType().getSize();
    }

    public Cell cellFromRow() {
        int estimatedSize = data.entrySet().stream()
                .mapToInt(entry -> estimateSize(entry.getKey(), entry.getValue()))
                .sum();
        ByteBuffer buffer = ByteBuffer.allocate(estimatedSize);
        for (Map.Entry<Column, Object> entry : data.entrySet()) {
            Column column = entry.getKey();
            Object value = entry.getValue();

            switch (column.dataType()) {
                case NULL -> buffer.put((byte) 0);
                case TINYINT, YEAR -> buffer.put((byte) value);
                case SMALLINT -> buffer.putShort((short) value);
                case INT -> buffer.putInt((Integer) value);
                case BIGINT, LONG -> buffer.putLong((Long) value);
                case FLOAT -> buffer.putFloat((Float) value);
                case DOUBLE -> buffer.putDouble((Double) value);
                case DATE, DATETIME -> buffer.putLong((long) value);
                case TEXT -> {
                    String strValue = (String) value;
                    buffer.put(strValue.getBytes());
                }
                default -> throw new UnsupportedOperationException(
                        "Unsupported data type: " + column.dataType());
            }
        }
        return TableCell.deserialize(buffer.array(), PageTypes.LEAF);
    }

    /**
     * Converts a Cell object back to a Row.
     */
    public static Row fromCell(Cell cell, Map<String, Column> columnSchema) {
        Map<Column, Object> rowData = new HashMap<>();
        ByteBuffer buffer = ByteBuffer.wrap(cell.cellPayload().serialize());

        for (Column column : columnSchema.values()) {
            switch (column.dataType()) {
                case NULL -> rowData.put(column, null);
                case TINYINT, YEAR -> rowData.put(column, buffer.get());
                case SMALLINT -> rowData.put(column, buffer.getShort());
                case INT -> rowData.put(column, buffer.getInt());
                case BIGINT, LONG -> rowData.put(column, buffer.getLong());
                case FLOAT -> rowData.put(column, buffer.getFloat());
                case DOUBLE -> rowData.put(column, buffer.getDouble());
                case DATE, DATETIME -> rowData.put(column, buffer.getLong());
                case TEXT -> {
                    byte[] textBytes = new byte[buffer.remaining()];
                    buffer.get(textBytes);
                    rowData.put(column, new String(textBytes));
                }
                default -> throw new UnsupportedOperationException("Unsupported data type: " + column.dataType());
            }
        }
        return new Row(rowData);
    }
}
