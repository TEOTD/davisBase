package com.neodymium.davisbase.models.table;

import com.neodymium.davisbase.constants.Constants;
import com.neodymium.davisbase.constants.enums.DataTypes;
import com.neodymium.davisbase.error.DavisBaseException;
import com.neodymium.davisbase.models.Cell;
import com.neodymium.davisbase.models.ConditionEvaluator;
import com.neodymium.davisbase.models.ConditionParser;
import com.neodymium.davisbase.models.index.Index;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.neodymium.davisbase.constants.Constants.TABLE_DIRECTORY;

@Data
@Slf4j
public class Table {
    private String tableName;
    private List<String> columnNames;
    private List<DataTypes> dataTypes;
    private Map<String, String> constraints;
    private Map<String, Index> indexes;
    private BPlusTree bPlusTree;
    private File tableFile;

    public Table(String tableName, String[] schema) throws IOException {
        this.tableName = tableName;
        this.columnNames = new ArrayList<>();
        this.dataTypes = new ArrayList<>();
        this.constraints = new HashMap<>();
        this.indexes = new HashMap<>();

        for (String columnDef : schema) {
            String[] parts = columnDef.trim().split("\\s+");
            columnNames.add(parts[0]);
            dataTypes.add(DataTypes.get(Byte.parseByte(parts[1])));
            if (parts.length > 2) {
                constraints.put(parts[0], String.join(" ", Arrays.copyOfRange(parts, 2, parts.length)));
                if (constraints.get(parts[0]).contains("PRIMARY KEY") || constraints.get(parts[0]).contains("UNIQUE")) {
                    indexes.put(parts[0], new Index(parts[0]));
                }
            }
        }

        this.tableFile = new File(TABLE_DIRECTORY + File.separator + tableName + ".tbl");
        if (!tableFile.exists()) {
            initializeTableFile();
        }

        this.bPlusTree = new BPlusTree(new RandomAccessFile(tableFile, "rw"));
    }

    /**
     * Checks if a table exists.
     */
    public static boolean exists(String tableName) {
        return new File(TABLE_DIRECTORY + File.separator + tableName + ".tbl").exists();
    }

    /**
     * Deletes or drop a table and its metadata.
     */
    public static void drop(String tableName) throws DavisBaseException, IOException {
        File file = new File(TABLE_DIRECTORY + File.separator + tableName + ".tbl");
        if (!file.exists() || !file.delete()) {
            throw new DavisBaseException("Failed to delete table '" + tableName + "'.");
        }
        removeFromMetadata(tableName);
        log.info("Table '{}' deleted successfully.", tableName);
    }

    /**
     * Removes table metadata from the system catalog.
     */
    private static void removeFromMetadata(String tableName) throws IOException {
        File metadataFile = new File(TABLE_DIRECTORY + File.separator + "davisbase_tables.tbl");
        List<String> lines = Files.readAllLines(metadataFile.toPath());
        lines.removeIf(line -> line.equalsIgnoreCase(tableName));
        Files.write(metadataFile.toPath(), lines);

        File columnsFile = new File(TABLE_DIRECTORY + File.separator + "davisbase_columns.tbl");
        lines = Files.readAllLines(columnsFile.toPath());
        lines.removeIf(line -> line.startsWith(tableName + "|"));
        Files.write(columnsFile.toPath(), lines);
    }

    /**
     * Creates a new table file with metadata.
     */
    public void create() throws IOException, DavisBaseException {
        if (tableFile.exists()) {
            throw new DavisBaseException("Table '" + tableName + "' already exists.");
        }

        if (!tableFile.createNewFile()) {
            throw new DavisBaseException("Failed to create table file: " + tableName);
        }

        log.info("Table '{}' created successfully.", tableName);
        initializeTableFile();
        addToMetadata();
    }

    /**
     * Inserts a new record into the table.
     */
    public void insert(Object[] values) throws IOException, DavisBaseException {
        if (values.length != columnNames.size()) {
            throw new DavisBaseException("Insert values do not match table schema.");
        }

        int rowId = generateRowId();
        TableRecord record = new TableRecord(
                new byte[1],
                rowId,
                columnNames,       // Include column names
                dataTypes,         // Include data types
                convertToBytes(values) // Serialized values
        );
        bPlusTree.insert(record.toCell());
        // Update indexes
        for (int i = 0; i < columnNames.size(); i++) {
            String column = columnNames.get(i);
            Index index = indexes.get(column);
            if (index != null) {
                index.insert(values[i], rowId);
            }
        }
        log.info("Record inserted into table '{}'.", tableName);
    }

    /**
     * Updates records matching a condition.
     */
    public int update(String condition, Map<String, Object> updates) throws IOException, DavisBaseException {
        log.info("Updating records in '{}' with condition: {} and updates: {}", tableName, condition, updates);

        Map<String, String> parsedConditions = ConditionParser.parseCondition(condition);
        List<Cell> matchingCells = bPlusTree.search(new ArrayList<>());

        int updatedCount = 0;
        for (Cell cell : matchingCells) {
            TableRecord record = TableRecord.fromCell(cell);
            if (ConditionEvaluator.evaluateRow(record, parsedConditions)) {
                // Remove old values from indexes
                for (int i = 0; i < columnNames.size(); i++) {
                    String column = columnNames.get(i);
                    Index index = indexes.get(column);
                    if (index != null) {
                        index.delete(record.getValue(column), record.getRowId());
                    }
                }

                // Apply updates
                for (Map.Entry<String, Object> update : updates.entrySet()) {
                    record.setValue(update.getKey(), update.getValue());
                }

                // Add new values to indexes
                for (int i = 0; i < columnNames.size(); i++) {
                    String column = columnNames.get(i);
                    Index index = indexes.get(column);
                    if (index != null) {
                        index.insert(record.getValue(column), record.getRowId());
                    }
                }

                // Update the record in the B+ Tree
                bPlusTree.update(record.toCell());
                updatedCount++;
            }
        }

        log.info("{} records updated in '{}'.", updatedCount, tableName);
        return updatedCount;
    }

    /**
     * Deletes records matching a condition.
     */
    public int delete(String condition) throws IOException, DavisBaseException {
        log.info("Deleting records from '{}' with condition: {}", tableName, condition);

        Map<String, String> parsedConditions = ConditionParser.parseCondition(condition);
        List<Cell> matchingCells = bPlusTree.search(new ArrayList<>());

        int deletedCount = 0;
        for (Cell cell : matchingCells) {
            TableRecord record = TableRecord.fromCell(cell);
            if (ConditionEvaluator.evaluateRow(record, parsedConditions)) {
                // Remove from indexes
                for (int i = 0; i < columnNames.size(); i++) {
                    String column = columnNames.get(i);
                    Index index = indexes.get(column);
                    if (index != null) {
                        index.delete(record.getValue(column), record.getRowId());
                    }
                }

                bPlusTree.delete(record.toCell());
                deletedCount++;
            }
        }

        log.info("{} records deleted from '{}'.", deletedCount, tableName);
        return deletedCount;
    }

    /**
     * Selects records matching a condition.
     */
    public List<Map<String, Object>> select(List<String> columns, String condition) throws IOException, DavisBaseException {
        log.info("Selecting columns '{}' from '{}' with condition: {}", columns, tableName, condition);

        Map<String, String> parsedConditions = ConditionParser.parseCondition(condition);
        List<Cell> matchingCells = bPlusTree.search(new ArrayList<>());

        List<Map<String, Object>> result = new ArrayList<>();
        for (Cell cell : matchingCells) {
            TableRecord record = TableRecord.fromCell(cell);
            if (ConditionEvaluator.evaluateRow(record, parsedConditions)) {
                Map<String, Object> row = new HashMap<>();
                for (String column : columns) {
                    row.put(column, record.getValue(column));
                }
                result.add(row);
            }
        }
        return result;
    }

    /**
     * Initializes the table file structure.
     */
    private void initializeTableFile() throws IOException {
        log.info("Initializing table file for '{}'.", tableName);
        bPlusTree.create();
    }

    /**
     * Adds table metadata to the system catalog.
     */
    private void addToMetadata() throws IOException {
        File metadataFile = new File(Constants.DATA_DIR + "davisbase_tables.tbl");
        if (!metadataFile.exists()) {
            metadataFile.createNewFile();
        }

        try (FileWriter writer = new FileWriter(metadataFile, true)) {
            writer.write(tableName + "\n");
        }

        File columnsFile = new File(Constants.DATA_DIR + "davisbase_columns.tbl");
        if (!columnsFile.exists()) {
            columnsFile.createNewFile();
        }

        try (FileWriter writer = new FileWriter(columnsFile, true)) {
            for (int i = 0; i < columnNames.size(); i++) {
                writer.write(tableName + "|" + columnNames.get(i) + "|" + dataTypes.get(i) + "\n");
            }
        }
    }

    /**
     * Converts values to byte arrays.
     */
    private List<byte[]> convertToBytes(Object[] values) {
        List<byte[]> result = new ArrayList<>();
        for (int i = 0; i < values.length; i++) {
            DataTypes type = dataTypes.get(i);
            result.add(serializeValue(type, values[i]));
        }
        return result;
    }

    private byte[] serializeValue(DataTypes type, Object value) {
        ByteBuffer buffer = ByteBuffer.allocate(DataTypes.getLength(type));

        switch (type) {
            case NULL:
                // No bytes needed for NULL
                return new byte[0];
            case TINYINT:
                buffer.put(((Number) value).byteValue());
                break;
            case SMALLINT:
                buffer.putShort(((Number) value).shortValue());
                break;
            case INT:
                buffer.putInt(((Number) value).intValue());
                break;
            case BIGINT:
                buffer.putLong(((Number) value).longValue());
                break;
            case FLOAT:
                buffer.putFloat(((Number) value).floatValue());
                break;
            case DOUBLE:
                buffer.putDouble(((Number) value).doubleValue());
                break;
            case YEAR:
                buffer.putShort(((Number) value).shortValue()); // Assuming YEAR as a 2-byte value
                break;
            case TIME:
            case DATETIME:
            case DATE:
                buffer.putLong(((Number) value).longValue()); // Assuming these are stored as timestamps
                break;
            case TEXT:
                return ((String) value).getBytes();
            default:
                throw new IllegalArgumentException("Unsupported data type: " + type);
        }

        return buffer.array();
    }


    /**
     * Generates a unique row ID.
     */
    private int generateRowId() {
        return (int) (System.currentTimeMillis() % Integer.MAX_VALUE);
    }
}
