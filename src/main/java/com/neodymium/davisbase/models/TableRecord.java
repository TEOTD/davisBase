package com.neodymium.davisbase.models;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class TableRecord {
    private final int rowId;                          // Unique row identifier
    private final Map<String, Object> data;          // Dynamic column-value map
    private final String primaryKeyColumn;           // Name of the primary key column

    /**
     * Constructor for TableRecord
     *
     * @param rowId            Row ID of the record
     * @param primaryKeyColumn The primary key column name
     * @param data             Map of column names to values
     */
    public TableRecord(int rowId, String primaryKeyColumn, Map<String, Object> data) {
        this.rowId = rowId;
        this.primaryKeyColumn = primaryKeyColumn;
        this.data = new HashMap<>(data);
    }

    /**
     * Serialize the record into a byte array for storage.
     *
     * @return Serialized byte array
     */
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(getSize());
        buffer.putInt(rowId); // Serialize row ID

        for (Map.Entry<String, Object> entry : data.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof Integer) {
                buffer.putInt((Integer) value);
            } else if (value instanceof Float) {
                buffer.putFloat((Float) value);
            } else if (value instanceof String) {
                byte[] strBytes = ((String) value).getBytes(StandardCharsets.UTF_8);
                buffer.putInt(strBytes.length); // Length of the string
                buffer.put(strBytes);          // Actual string bytes
            } else if (value == null) {
                buffer.putInt(-1); // Null indicator
            }
        }
        return buffer.array();
    }

    /**
     * Update a specific field's value.
     *
     * @param fieldName Field name to update
     * @param value     New value
     */
    public void updateField(String fieldName, Object value) {
        if (!data.containsKey(fieldName)) {
            throw new IllegalArgumentException("Field " + fieldName + " does not exist.");
        }
        data.put(fieldName, value);
    }

    /**
     * Check if the record matches the given condition (e.g., "age > 18").
     *
     * @param condition Condition to evaluate
     * @return True if the record matches the condition, false otherwise
     */
    public boolean matchesCondition(String condition) {
        String[] parts = condition.split("\\s+");
        if (parts.length != 3) {
            throw new IllegalArgumentException("Invalid condition format: " + condition);
        }

        String field = parts[0];
        String operator = parts[1];
        String value = parts[2];

        if (!data.containsKey(field)) {
            throw new IllegalArgumentException("Field " + field + " does not exist.");
        }

        Object fieldValue = data.get(field);
        if (fieldValue instanceof Integer) {
            return compare((Integer) fieldValue, operator, Integer.parseInt(value));
        } else if (fieldValue instanceof Float) {
            return compare((Float) fieldValue, operator, Float.parseFloat(value));
        } else if (fieldValue instanceof String) {
            return compare((String) fieldValue, operator, value);
        } else {
            return false;
        }
    }

    /**
     * Get the primary key value as a string.
     *
     * @return Primary key value
     */
    public String getPrimaryKey() {
        return String.valueOf(data.get(primaryKeyColumn));
    }

    /**
     * Get the row ID.
     *
     * @return Row ID
     */
    public int getRowId() {
        return rowId;
    }

    /**
     * Get the record's size in bytes.
     *
     * @return Record size in bytes
     */
    public int getSize() {
        int size = 4; // For rowId
        for (Object value : data.values()) {
            if (value instanceof Integer || value instanceof Float) {
                size += 4;
            } else if (value instanceof String) {
                size += 4 + ((String) value).getBytes(StandardCharsets.UTF_8).length; // String length + content
            } else if (value == null) {
                size += 4; // Null indicator
            }
        }
        return size;
    }

    /**
     * Get the record's data as a map.
     *
     * @return Column-value map
     */
    public Map<String, Object> getData() {
        return new HashMap<>(data);
    }

    private boolean compare(int fieldValue, String operator, int value) {
        switch (operator) {
            case "=":
                return fieldValue == value;
            case ">":
                return fieldValue > value;
            case ">=":
                return fieldValue >= value;
            case "<":
                return fieldValue < value;
            case "<=":
                return fieldValue <= value;
            case "!=":
                return fieldValue != value;
            default:
                throw new IllegalArgumentException("Invalid operator: " + operator);
        }
    }

    private boolean compare(float fieldValue, String operator, float value) {
        switch (operator) {
            case "=":
                return fieldValue == value;
            case ">":
                return fieldValue > value;
            case ">=":
                return fieldValue >= value;
            case "<":
                return fieldValue < value;
            case "<=":
                return fieldValue <= value;
            case "!=":
                return fieldValue != value;
            default:
                throw new IllegalArgumentException("Invalid operator: " + operator);
        }
    }

    private boolean compare(String fieldValue, String operator, String value) {
        switch (operator) {
            case "=":
                return fieldValue.equals(value);
            case "!=":
                return !fieldValue.equals(value);
            default:
                throw new IllegalArgumentException("Invalid operator for String: " + operator);
        }
    }

    @Override
    public String toString() {
        return "TableRecord{" +
                "rowId=" + rowId +
                ", data=" + data +
                '}';
    }
}