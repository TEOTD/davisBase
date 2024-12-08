package com.neodymium.davisbase.models.table;

import com.neodymium.davisbase.constants.Constants;
import com.neodymium.davisbase.constants.enums.Constraints;
import com.neodymium.davisbase.error.DavisBaseException;
import com.neodymium.davisbase.models.Cell;
import com.neodymium.davisbase.models.ConditionEvaluator;
import com.neodymium.davisbase.models.ConditionParser;
import com.neodymium.davisbase.models.index.BTree;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.*;

/**
 * Represents a table in DavisBase.
 */
@Getter
@Setter
@Slf4j
public class Table {
    private String tableName;
    private List<Column> columns;
    private Map<String, BTree> indexes;
    private BPlusTree bPlusTree;
    private File tableFile;
    private String primaryKey;
    private static final int PAGE_SIZE = 512;

    public Table(String tableName, Column[] schema) throws IOException {
        this.tableName = tableName;
        this.columns = Arrays.asList(schema);
        this.indexes = new HashMap<>();

        // Determine the primary key
        this.primaryKey = Arrays.stream(schema)
                .filter(column -> column.constraint() == Constraints.PRIMARY_KEY)
                .map(Column::name)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Table must have a PRIMARY KEY column"));

        this.tableFile = new File(Constants.DATA_DIR + tableName + ".tbl");
        if (!tableFile.exists()) {
            initializeTableFile();
        }

        this.bPlusTree = new BPlusTree(new RandomAccessFile(tableFile, "rw"));

        // Create the primary key index by default
        createIndex(primaryKey);

        // Register metadata
        addToMetadata();
    }

    /**
     * Checks if a table exists.
     */
    public static boolean exists(String tableName) {
        return new File(Constants.DATA_DIR + tableName + ".tbl").exists();
    }

    /**
     * Creates an index for a specified column.
     */
    public void createIndex(String columnName) throws IOException {
        File indexFile = new File(Constants.DATA_DIR + tableName + "_" + columnName + ".idx");
        if (!indexFile.exists()) {
            if (!indexFile.createNewFile()) {
                throw new IOException("Failed to create index file for column: " + columnName);
            }
        }
        indexes.put(columnName, new BTree(new RandomAccessFile(indexFile, "rw")));
        log.info("Index created for column '{}'.", columnName);
    }

    /**
     * Deletes an index for a specified column.
     */
    public void dropIndex(String columnName) throws IOException {
        File indexFile = new File(Constants.DATA_DIR + tableName + "_" + columnName + ".idx");
        if (indexFile.exists() && !indexFile.delete()) {
            throw new IOException("Failed to delete index file for column: " + columnName);
        }
        indexes.remove(columnName);
        log.info("Index for column '{}' dropped.", columnName);
    }

    /**
     * Inserts a new record into the table.
     */
    public void insert(Map<String, Object> values) throws IOException {
        Map<Column, Object> rowData = new HashMap<>();
        for (Column column : columns) {
            Object value = values.get(column.name());
            if (value == null && column.constraint() == Constraints.NOT_NULL) {
                throw new DavisBaseException("NOT NULL constraint violated for column: " + column.name());
            }
            rowData.put(column, value);
        }

        Row row = new Row(rowData);
        Object primaryKeyValue = values.get(primaryKey);

        if (primaryKeyValue == null) {
            throw new DavisBaseException("Primary key value cannot be null.");
        }

        Map<String, Cell> cellMap = new HashMap<>();
        cellMap.put(primaryKeyValue.toString(), row.cellFromRow());
        bPlusTree.insert(cellMap);

        for (Map.Entry<String, BTree> indexEntry : indexes.entrySet()) {
            String indexColumn = indexEntry.getKey();
            BTree bTree = indexEntry.getValue();
            bTree.insert(primaryKeyValue, values.get(indexColumn));
        }

        log.info("Record inserted into table '{}'.", tableName);
    }

    /**
     * Updates records matching a condition.
     */
    public int update(String condition, Map<String, Object> updates) throws IOException {
        Map<String, String> parsedConditions = ConditionParser.parseCondition(condition);
        List<Cell> matchingCells = bPlusTree.search();

        int updatedCount = 0;
        for (Cell cell : matchingCells) {
            Row row = Row.fromCell(cell, getColumnSchema());

            if (ConditionEvaluator.evaluateRow(row, parsedConditions)) {
                Object primaryKeyValue = row.data().get(getColumnByName(primaryKey));
                for (Map.Entry<String, Object> update : updates.entrySet()) {
                    Column column = getColumnByName(update.getKey());
                    Object oldValue = row.data().get(column);
                    Object newValue = update.getValue();

                    row.data().put(column, newValue);

                    if (indexes.containsKey(column.name())) {
                        indexes.get(column.name()).update(oldValue, newValue, primaryKeyValue);
                    }
                }

                bPlusTree.update(row.cellFromRow(), primaryKeyValue.toString());
                updatedCount++;
            }
        }

        log.info("{} records updated in '{}'.", updatedCount, tableName);
        return updatedCount;
    }

    /**
     * Deletes records matching a condition.
     */
    public int delete(String condition) throws IOException {
        Map<String, String> parsedConditions = ConditionParser.parseCondition(condition);
        List<Cell> matchingCells = bPlusTree.search();

        int deletedCount = 0;
        for (Cell cell : matchingCells) {
            Row row = Row.fromCell(cell, getColumnSchema());
            if (ConditionEvaluator.evaluateRow(row, parsedConditions)) {
                Object primaryKeyValue = row.data().get(getColumnByName(primaryKey));
                bPlusTree.delete(cell.cellHeader().rowId());
                deletedCount++;
                // deleting index as per that deleted cell
                /*
                for (Map.Entry<String, BTree> indexEntry : indexes.entrySet()) {
                    String indexColumn = indexEntry.getKey();
                    BTree bTree = indexEntry.getValue();
                    bTree.delete(primaryKeyValue, row.data().get(getColumnByName(indexColumn)));
                }

                 */
            }
        }

        log.info("{} records deleted from '{}'.", deletedCount, tableName);
        return deletedCount;
    }

    /**
     * Selects records matching a condition.
     */
    public List<Map<Column, Object>> select(List<String> columnNames, String condition) throws IOException {
        Map<String, String> parsedConditions = ConditionParser.parseCondition(condition);
        List<Cell> matchingCells = bPlusTree.search();

        List<Map<Column, Object>> result = new ArrayList<>();
        for (Cell cell : matchingCells) {
            Row row = Row.fromCell(cell, getColumnSchema());
            if (ConditionEvaluator.evaluateRow(row, parsedConditions)) {
                Map<Column, Object> selectedRow = new HashMap<>();
                for (String columnName : columnNames) {
                    Column column = getColumnByName(columnName);
                    selectedRow.put(column, row.data().get(column));
                }
                result.add(selectedRow);
            }
        }

        return result;
    }

    /**
     * Initializes the table file structure.
     */
    private void initializeTableFile() throws IOException {
        bPlusTree.create();
        log.info("Table file initialized for '{}'.", tableName);
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
            for (Column column : columns) {
                writer.write(tableName + "|" + column.name() + "|" + column.dataType() + "\n");
            }
        }
    }


    /**
     * Retrieves a column schema map for use in deserialization.
     */
    private Map<String, Column> getColumnSchema() {
        Map<String, Column> columnSchema = new HashMap<>();
        for (Column column : columns) {
            columnSchema.put(column.name(), column);
        }
        return columnSchema;
    }

    /**
     * Retrieves a column object by name.
     */
    private Column getColumnByName(String columnName) {
        return columns.stream()
                .filter(column -> column.name().equals(columnName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Column '" + columnName + "' does not exist."));
    }
    /**
     * Drops a table, deleting the table file, all associated index files, and removing metadata.
     */
    public static void dropTable(String tableName) throws IOException {
        log.info("Dropping table '{}'.", tableName);

        // Delete the table file
        File tableFile = new File(Constants.DATA_DIR + tableName + ".tbl");
        if (tableFile.exists() && !tableFile.delete()) {
            throw new IOException("Failed to delete table file for table: " + tableName);
        }

        // Delete all associated index files
        File dataDir = new File(Constants.DATA_DIR);
        File[] indexFiles = dataDir.listFiles((dir, name) -> name.startsWith(tableName + "_") && name.endsWith(".idx"));
        if (indexFiles != null) {
            for (File indexFile : indexFiles) {
                if (!indexFile.delete()) {
                    throw new IOException("Failed to delete index file: " + indexFile.getName());
                }
            }
        }

        // Remove metadata entries
        removeFromMetadata(tableName);

        log.info("Table '{}' and its associated metadata have been successfully dropped.", tableName);
    }

    /**
     * Removes table metadata from the system catalog.
     */
    private static void removeFromMetadata(String tableName) throws IOException {
        // Remove from davisbase_tables.tbl
        File metadataFile = new File(Constants.DATA_DIR + "davisbase_tables.tbl");
        if (metadataFile.exists()) {
            List<String> lines = Files.readAllLines(metadataFile.toPath());
            lines.removeIf(line -> line.equalsIgnoreCase(tableName));
            Files.write(metadataFile.toPath(), lines);
        }

        // Remove from davisbase_columns.tbl
        File columnsFile = new File(Constants.DATA_DIR + "davisbase_columns.tbl");
        if (columnsFile.exists()) {
            List<String> lines = Files.readAllLines(columnsFile.toPath());
            lines.removeIf(line -> line.startsWith(tableName + "|"));
            Files.write(columnsFile.toPath(), lines);
        }

        log.info("Metadata for table '{}' removed from the system catalog.", tableName);
    }
}
