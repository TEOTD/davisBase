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
 * Represents a table in DavisBase using BTree.
 */
@Getter
@Setter
@Slf4j
public class Table {
    private String tableName;
    private List<Column> columns;
    private Map<String, BTree> indexes; // Indexes for columns
    private BTree bTree; // The main BTree for the table
    private File tableFile; // File representing the table's storage
    private String primaryKey; // Primary key column name

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

        this.bTree = new BTree(new RandomAccessFile(tableFile, "rw"));

        // Create the primary key index by default
        createIndex(primaryKey);

        // Register metadata
        addToMetadata();
    }

    public static boolean exists(String tableName) {
        return new File(Constants.DATA_DIR + tableName + ".tbl").exists();
    }

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

    public void dropIndex(String columnName) throws IOException {
        File indexFile = new File(Constants.DATA_DIR + tableName + "_" + columnName + ".idx");
        if (indexFile.exists() && !indexFile.delete()) {
            throw new IOException("Failed to delete index file for column: " + columnName);
        }
        indexes.remove(columnName);
        log.info("Index for column '{}' dropped.", columnName);
    }

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

        Cell cell = row.cellFromRow();

        bTree.insert(cell.cellHeader().rowId(), cell); // Insert into the BTree

        for (Map.Entry<String, BTree> indexEntry : indexes.entrySet()) {
            String indexColumn = indexEntry.getKey();
            BTree bTreeIndex = indexEntry.getValue();
            bTreeIndex.insert(primaryKeyValue, values.get(indexColumn));
        }

        log.info("Record inserted into table '{}'.", tableName);
    }

    public int update(String condition, Map<String, Object> updates) throws IOException {
        Map<String, String> parsedConditions = ConditionParser.parseCondition(condition);

        List<Cell> matchingCells = bTree.search(); // Search all cells in the BTree

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

                bTree.update(row.cellFromRow(), primaryKeyValue.toString());
                updatedCount++;
            }
        }

        log.info("{} records updated in '{}'.", updatedCount, tableName);

        return updatedCount;
    }

    public int delete(String condition) throws IOException {
        Map<String, String> parsedConditions = ConditionParser.parseCondition(condition);

        List<Cell> matchingCells = bTree.search(); // Search all cells in the BTree

        int deletedCount = 0;

        for (Cell cell : matchingCells) {
            Row row = Row.fromCell(cell, getColumnSchema());

            if (ConditionEvaluator.evaluateRow(row, parsedConditions)) {
                Object primaryKeyValue = row.data().get(getColumnByName(primaryKey));

                bTree.delete(cell.cellHeader().rowId()); // Delete from BTree

                deletedCount++;
            }
        }

        log.info("{} records deleted from '{}'.", deletedCount, tableName);

        return deletedCount;
    }

    public List<Map<Column, Object>> select(List<String> columnNames, String condition) throws IOException {
        Map<String, String> parsedConditions = ConditionParser.parseCondition(condition);

        List<Cell> matchingCells = bTree.search(); // Search all cells in the BTree

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

    private void initializeTableFile() throws IOException {
       bTree.create(); // Initialize the root node of the BTree
       log.info("Table file initialized for '{}'.", tableName);
   }

   private void addToMetadata() throws IOException {
       // Add metadata logic here similar to the provided implementation.
       // This includes adding entries to davisbase_tables.tbl and davisbase_columns.tbl.
   }

   private Map<String, Column> getColumnSchema() {
       Map<String, Column> columnSchema = new HashMap<>();
       for (Column column : columns) {
           columnSchema.put(column.name(), column);
       }
       return columnSchema;
   }

   private Column getColumnByName(String columnName) {
       return columns.stream()
               .filter(column -> column.name().equals(columnName))
               .findFirst()
               .orElseThrow(() -> new IllegalArgumentException("Column '" + columnName + "' does not exist."));
   }
}
