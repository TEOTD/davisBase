package com.neodymium.davisbase.models;

import com.neodymium.davisbase.constants.enums.DataTypes;
import com.neodymium.davisbase.constants.enums.PageTypes;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

@Data
@AllArgsConstructor
public class TableRecord implements Record {
    private byte[] deletionFlag;
    private int rowId;
    private List<String> columnNames; // Column names
    private List<DataTypes> dataTypes; // Data types for each column
    private List<byte[]> body; // Serialized values

    /**
     * Marks the record as deleted.
     */
    @Override
    public void delete() {
        if (deletionFlag == null || deletionFlag.length == 0) {
            deletionFlag = new byte[1];
        }
        deletionFlag[0] = 1; // Mark as deleted
    }

    /**
     * Checks if the record exists (not deleted).
     */
    @Override
    public boolean exists() {
        return deletionFlag == null || deletionFlag[0] == 0;
    }

    /**
     * Serializes the record to a byte array.
     */
    @Override
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(calculateSize());
        buffer.put(deletionFlag);
        buffer.putInt(rowId);
        buffer.putInt(body.size()); // Number of columns
        for (DataTypes type : dataTypes) {
            buffer.put(type.getValue());
        }
        for (byte[] value : body) {
            buffer.put(value);
        }
        return buffer.array();
    }

    /**
     * Calculates the size of the serialized record.
     */
    private int calculateSize() {
        int size = 1 + Integer.BYTES + Integer.BYTES; // Deletion flag, rowId, number of columns
        size += dataTypes.size(); // Data type codes
        for (byte[] value : body) {
            size += value.length;
        }
        return size;
    }

    /**
     * Retrieves the value of a column by name.
     */
    public Object getValue(String columnName) {
        int columnIndex = columnNames.indexOf(columnName);
        if (columnIndex == -1) {
            throw new IllegalArgumentException("Column '" + columnName + "' does not exist in the table schema.");
        }
        return getValue(columnIndex);
    }

    /**
     * Retrieves the value of a column by index.
     */
    public Object getValue(int columnIndex) {
        if (columnIndex < 0 || columnIndex >= body.size()) {
            throw new IllegalArgumentException("Invalid column index: " + columnIndex);
        }
        return deserializeValue(dataTypes.get(columnIndex), body.get(columnIndex));
    }

    /**
     * Deserializes a value based on the column's data type.
     */
    private Object deserializeValue(DataTypes dataType, byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        switch (dataType) {
            case NULL:
                return null;
            case TINYINT:
                return buffer.get();
            case SMALLINT:
                return buffer.getShort();
            case INT:
                return buffer.getInt();
            case BIGINT:
                return buffer.getLong();
            case FLOAT:
                return buffer.getFloat();
            case DOUBLE:
                return buffer.getDouble();
            case TEXT:
                return new String(data);
            default:
                throw new IllegalArgumentException("Unsupported data type: " + dataType);
        }
    }

    /**
     * Converts a `Cell` to a `TableRecord`.
     */
    public static TableRecord fromCell(Cell cell) {
        ByteBuffer buffer = ByteBuffer.wrap(cell.serialize());

        // Extract deletion flag
        byte[] deletionFlag = new byte[1];
        buffer.get(deletionFlag);

        // Extract row ID
        int rowId = buffer.getInt();

        // Extract number of columns
        int noOfColumns = buffer.getInt();

        // Extract data types
        List<DataTypes> dataTypes = new ArrayList<>();
        for (int i = 0; i < noOfColumns; i++) {
            byte typeCode = buffer.get();
            dataTypes.add(DataTypes.get(typeCode));
        }

        // Extract body (values)
        List<byte[]> body = new ArrayList<>();
        for (DataTypes type : dataTypes) {
            int size = type == DataTypes.TEXT ? buffer.remaining() : DataTypes.getLength(type);
            byte[] value = new byte[size];
            buffer.get(value);
            body.add(value);
        }

        return new TableRecord(deletionFlag, rowId, new ArrayList<>(), dataTypes, body); // Updated with empty columnNames
    }

    /**
     * Converts this record to a `Cell`.
     */
    public Cell toCell() {
        return new TableCell(
                calculateSize(),
                rowId,
                PageTypes.LEAF,
                (short) 0,
                this,
                String.valueOf(rowId) // Primary key representation
        );
    }

    /**
     * Updates the value of a column by name.
     */
    public void setValue(String columnName, Object value) {
        int columnIndex = columnNames.indexOf(columnName);
        if (columnIndex == -1) {
            throw new IllegalArgumentException("Column '" + columnName + "' does not exist.");
        }
        setValue(columnIndex, value);
    }

    /**
     * Updates the value of a column by index.
     */
    public void setValue(int columnIndex, Object value) {
        if (columnIndex < 0 || columnIndex >= body.size()) {
            throw new IllegalArgumentException("Invalid column index: " + columnIndex);
        }
        DataTypes type = dataTypes.get(columnIndex);
        body.set(columnIndex, serializeValue(type, value));
    }

    /**
     * Serializes a value based on its data type.
     */
    private byte[] serializeValue(DataTypes type, Object value) {
        ByteBuffer buffer = ByteBuffer.allocate(DataTypes.getLength(type));
        switch (type) {
            case INT:
                buffer.putInt((int) value);
                break;
            case FLOAT:
                buffer.putFloat((float) value);
                break;
            case TEXT:
                return ((String) value).getBytes();
            default:
                throw new IllegalArgumentException("Unsupported data type: " + type);
        }
        return buffer.array();
    }
}
