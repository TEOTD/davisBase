package com.neodymium.davisbase.models.table;

import com.neodymium.davisbase.constants.enums.Constraints;
import com.neodymium.davisbase.constants.enums.DataTypes;
import com.neodymium.davisbase.error.DavisBaseException;
import com.neodymium.davisbase.models.Cell;
import com.neodymium.davisbase.models.ConditionEvaluator;
import com.neodymium.davisbase.models.ConditionParser;
import com.neodymium.davisbase.models.index.Index;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.ObjectUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.neodymium.davisbase.constants.Constants.COLUMN_CATALOG;
import static com.neodymium.davisbase.constants.Constants.INDEX_DIRECTORY;
import static com.neodymium.davisbase.constants.Constants.INDEX_FILE_EXTENSION;
import static com.neodymium.davisbase.constants.Constants.TABLE_CATALOG;
import static com.neodymium.davisbase.constants.Constants.TABLE_DIRECTORY;
import static com.neodymium.davisbase.constants.Constants.TABLE_FILE_EXTENSION;

/**
 * Represents a table in the DavisBase database.
 */
@Getter
@Setter
@Slf4j
public class Table {
    private final String tableName;
    private final List<Column> columns = new ArrayList<>();
    private final Map<String, Index> indexes = new HashMap<>();
    private final Map<String, String> indexColMap = new HashMap<>(); // key -> index name && value -> column name
    private final BPlusTree bPlusTree;
    private final String primaryKey;

    /**
     * Constructor to create or load a table.
     *
     * @param tableName Name of the table.
     * @param schema    Array of columns defining the table schema.
     * @throws IOException If an I/O error occurs.
     */
    public Table(String tableName, List<Column> schema) throws IOException {
        this.tableName = tableName;

        // Identify the primary key
        this.primaryKey = schema.stream()
                .filter(Column::isPrimaryKey)
                .map(Column::name)
                .findFirst()
                .orElse(null);

        File tableFile = new File(TABLE_DIRECTORY, tableName + TABLE_FILE_EXTENSION);
        if (!tableFile.exists()) {
            initializeTableFile(tableFile);
        }

        this.bPlusTree = new BPlusTree(new RandomAccessFile(tableFile, "rw"));
        if (!ObjectUtils.isEmpty(this.primaryKey)) {
            createIndex(INDEX_DIRECTORY + tableName + primaryKey + INDEX_FILE_EXTENSION, primaryKey);
        }
        if (!schema.isEmpty()) {
            initializeColumnsFromSchema(schema);
            addToMetadata();
        } else {
            initializeColumnsFromMetadata();
        }
    }

    /**
     * Drops a table by deleting its metadata and associated files.
     *
     * @throws IOException If there is an error accessing files.
     */
    public static void drop(String tableName) throws IOException {
        log.info("Dropping table '{}'.", tableName);

        // Step 1: Delete the table file
        File tableFile = new File(TABLE_DIRECTORY, tableName + TABLE_FILE_EXTENSION);
        if (tableFile.exists() && !tableFile.delete()) {
            throw new IOException("Failed to delete table file for table: " + tableName);
        }

        // Step 3: Remove metadata from davisbase_tables
        File tablesCatalog = new File(TABLE_CATALOG);
        if (tablesCatalog.exists()) {
            List<String> tableLines = Files.readAllLines(tablesCatalog.toPath());
            tableLines.removeIf(line -> line.split("\\|")[1].equalsIgnoreCase(tableName));
            Files.write(tablesCatalog.toPath(), tableLines);
        }

        // Step 4: Remove metadata from davisbase_columns
        File columnsCatalog = new File(COLUMN_CATALOG);
        if (columnsCatalog.exists()) {
            List<String> columnLines = Files.readAllLines(columnsCatalog.toPath());
            columnLines.removeIf(line -> line.split("\\|")[3].equalsIgnoreCase(tableName));
            Files.write(columnsCatalog.toPath(), columnLines);
        }

        log.info("Table '{}' and its associated metadata have been successfully dropped.", tableName);
    }

    /**
     * Initializes the table file with a root page.
     */
    private void initializeTableFile(File tableFile) throws IOException {
        if (tableFile.createNewFile()) {
            bPlusTree.create();
            log.info("Table file created for '{}'.", tableName);
        } else {
            throw new IOException("Failed to initialize table file: " + tableFile.getName());
        }
    }

    /**
     * Initializes columns for a new table based on the given schema.
     */
    private void initializeColumnsFromSchema(List<Column> schemaInfo) {
        for (Column column : schemaInfo) {
            addColumn(column.name(), column.dataType(), column.constraints());
        }
    }

    /**
     * Initializes columns for an existing table by reading metadata.
     */
    private void initializeColumnsFromMetadata() throws IOException {
        File columnsCatalog = new File(COLUMN_CATALOG);
        if (!columnsCatalog.exists()) {
            throw new IllegalStateException("Metadata file does not exist: " + COLUMN_CATALOG);
        }

        List<String> metadataLines = Files.readAllLines(columnsCatalog.toPath());
        for (String line : metadataLines) {
            String[] parts = line.split("\\|");
            if (parts.length != 7) continue;

            String tableNameFromMeta = parts[3]; // Column 4 is the table name
            if (tableName.equalsIgnoreCase(tableNameFromMeta)) {
                String columnName = parts[1];
                DataTypes dataType = DataTypes.valueOf(parts[4]);
                Set<Constraints> constraints = parseConstraints(parts[6]);
                addColumn(columnName, dataType, constraints);
            }
        }
    }

    /**
     * Adds a column to the schema.
     */
    private void addColumn(String name, DataTypes dataType, Set<Constraints> constraints) {
        if (constraints.contains(Constraints.PRIMARY_KEY) && this.primaryKey != null) {
            throw new IllegalArgumentException("Table can have only one PRIMARY KEY column.");
        }

        columns.add(new Column(name, dataType, constraints));
    }

    /**
     * Adds the table metadata to the system catalogs.
     */
    private void addToMetadata() throws IOException {
        // Add an entry to davisbase_tables
        File tablesCatalogFile = new File(TABLE_CATALOG);
        if (!tablesCatalogFile.exists()) {
            tablesCatalogFile.createNewFile();
        }

        try (FileWriter tableWriter = new FileWriter(tablesCatalogFile, true)) {
            int recordCount = 0; // New table starts with zero records
            short avgLength = 0; // Optional column, default to 0
            short rootPage = 0;  // Optional column, default to 0
            String row = String.format("%d|%s|%d|%d|%d\n",
                    getNextRowId(tablesCatalogFile), tableName, recordCount, avgLength, rootPage);
            tableWriter.write(row);
        }

        // Add an entry to davisbase_columns for each column
        File columnsCatalogFile = new File(COLUMN_CATALOG);
        if (!columnsCatalogFile.exists()) {
            columnsCatalogFile.createNewFile();
        }

        try (FileWriter columnWriter = new FileWriter(columnsCatalogFile, true)) {
            int tableRowId = getTableRowId(tableName, tablesCatalogFile); // Obtain the row ID of the table
            int ordinalPosition = 1;
            for (Column column : columns) {
                String constraints = String.join(",", column.constraints().stream()
                        .map(Constraints::getValue)
                        .toList());
                String row = String.format("%d|%s|%d|%s|%s|%d|%d\n",
                        getNextRowId(columnsCatalogFile), column.name(), tableRowId, tableName,
                        column.dataType().toString(), ordinalPosition++, constraints);
                columnWriter.write(row);
            }
        }
    }

    private Set<Constraints> parseConstraints(String constraintsString) {
        Set<Constraints> constraints = new HashSet<>();
        if (constraintsString == null || constraintsString.isEmpty()) {
            return constraints;
        }
        for (String constraint : constraintsString.split(",")) {
            constraints.add(Constraints.valueOf(constraint));
        }
        return constraints;
    }

    /**
     * Retrieves the next row ID for a catalog table.
     */
    private int getNextRowId(File catalogFile) throws IOException {
        List<String> lines = Files.readAllLines(catalogFile.toPath());
        return lines.size() + 1; // Assuming row IDs are sequential
    }

    /**
     * Retrieves the row ID of the table from davisbase_tables.
     */
    private int getTableRowId(String tableName, File tablesCatalogFile) throws IOException {
        List<String> lines = Files.readAllLines(tablesCatalogFile.toPath());
        for (String line : lines) {
            String[] parts = line.split("\\|");
            if (parts[1].equalsIgnoreCase(tableName)) {
                return Integer.parseInt(parts[0]);
            }
        }
        throw new IllegalArgumentException("Table '" + tableName + "' not found in davisbase_tables.");
    }

    /**
     * Inserts a record into the table.
     */
    public void insert(Map<String, Object> values) throws IOException {
        Map<Column, Object> rowData = new HashMap<>();
        for (Column column : columns) {
            Object value = values.get(column.name());

            // Check NOT NULL constraint
            if (value == null && column.constraints().contains(Constraints.NOT_NULL)) {
                throw new DavisBaseException("NOT NULL constraint violated for column: " + column.name());
            }

            // Check PRIMARY KEY constraint
            if (column.constraints().contains(Constraints.PRIMARY_KEY)) {
                if (value == null) {
                    throw new DavisBaseException("PRIMARY KEY constraint violated: value cannot be null for column: " + column.name());
                }
                if (bPlusTree.search(value.toString()) != null) {
                    throw new DavisBaseException("PRIMARY KEY constraint violated: duplicate value found for column: " + column.name());
                }
            }

            // Check UNIQUE constraint
            if (column.constraints().contains(Constraints.UNIQUE)) {
                if (bPlusTree.searchByColumn(column.name(), value) != null) { // Assuming `searchByColumn` checks for uniqueness
                    throw new DavisBaseException("UNIQUE constraint violated for column: " + column.name());
                }
            }

            rowData.put(column, value);
        }

        Row row = new Row(bPlusTree.getMaxRowId() + 1, rowData);
        bPlusTree.insert(Collections.singletonMap(this.primaryKey, row.cellFromRow()));

        /*
        // Update indexes
        if(!StringUtils.isEmpty(this.primaryKey)){
            for (Map.Entry<String, BPlusTree> index : indexes.entrySet()) {
                String columnName = index.getKey();
                index.getValue().insert(this.primaryKey, values.get(columnName));
            }
        }

         */
        log.info("Record inserted into table '{}'.", tableName);
    }

    /**
     * Selects records based on conditions.
     */
    public List<Map<String, Object>> select(List<String> columnNames, String condition) throws IOException {
        // Parse the condition into a structured format
        Map<String, String> parsedConditions = ConditionParser.parseCondition(condition);

        // Retrieve all matching cells from the B+-tree
        List<Cell> matchingCells = bPlusTree.search();

        // Prepare the result list
        List<Map<String, Object>> result = new ArrayList<>();

        // Iterate over matching cells
        for (Cell cell : matchingCells) {
            Row row = Row.fromCell(cell, getColumnSchema());

            // Check if the row matches the parsed conditions
            if (ConditionEvaluator.evaluateRow(row, parsedConditions)) {
                Map<String, Object> selectedRow = new HashMap<>();

                // Collect specified columns
                for (String columnName : columnNames) {
                    Column column = getColumnByName(columnName);
                    if (column == null) {
                        throw new IllegalArgumentException("Column '" + columnName + "' does not exist.");
                    }
                    selectedRow.put(column.name(), row.data().get(column));
                }

                result.add(selectedRow);
            }
        }
        return result;
    }

    /**
     * Updates records based on conditions.
     */
    public int update(String condition, Map<String, Object> updates) throws IOException {
        Map<String, String> parsedConditions = ConditionParser.parseCondition(condition);
        List<Cell> matchingCells = bPlusTree.search();

        int updatedCount = 0;
        for (Cell cell : matchingCells) {
            Row row = Row.fromCell(cell, getColumnSchema());
            if (ConditionEvaluator.evaluateRow(row, parsedConditions)) {
                //Object primaryKeyValue = row.data().get(getColumnByName(primaryKey));

                // Apply updates and propagate to indexes
                for (Map.Entry<String, Object> update : updates.entrySet()) {
                    Column column = getColumnByName(update.getKey());
                    Object newValue = update.getValue();
                    // Check NOT NULL constraint
                    if (newValue == null && column.constraints().contains(Constraints.NOT_NULL)) {
                        throw new DavisBaseException("NOT NULL constraint violated for column: " + column.name());
                    }

                    // Check PRIMARY KEY constraint
                    if (column.constraints().contains(Constraints.PRIMARY_KEY)) {
                        if (newValue == null) {
                            throw new DavisBaseException("PRIMARY KEY constraint violated: value cannot be null for column: " + column.name());
                        }
                        if (!newValue.equals(row.data().get(column)) && bPlusTree.search(newValue.toString()) != null) {
                            throw new DavisBaseException("PRIMARY KEY constraint violated: duplicate value found for column: " + column.name());
                        }
                    }

                    // Check UNIQUE constraint
                    if (column.constraints().contains(Constraints.UNIQUE)) {
                        if (!newValue.equals(row.data().get(column)) && bPlusTree.searchByColumn(column.name(), newValue) != null) {
                            throw new DavisBaseException("UNIQUE constraint violated for column: " + column.name());
                        }
                    }
                    row.data().put(column, newValue);
                    /*
                    if (indexes.containsKey(column.name())) {
                        indexes.get(column.name()).update(primaryKeyValue.toString(), newValue);
                    }

                     */
                }
                bPlusTree.update(row.cellFromRow(), null);
                updatedCount++;
            }
        }
        return updatedCount;
    }

    /**
     * Deletes records based on conditions.
     */
    public int delete(String condition) throws IOException {
        // Parse the condition
        Map<String, String> parsedConditions = ConditionParser.parseCondition(condition);

        // Retrieve all matching cells from the B+-tree
        List<Cell> matchingCells = bPlusTree.search();

        int deletedCount = 0;

        // Iterate over matching cells
        for (Cell cell : matchingCells) {
            Row row = Row.fromCell(cell, getColumnSchema());

            // Check if the row matches the parsed conditions
            if (ConditionEvaluator.evaluateRow(row, parsedConditions)) {
                //Object primaryKeyValue = row.data().get(getColumnByName(primaryKey).orElseThrow());

                // Delete the row from the B+-tree
                bPlusTree.delete(cell.cellHeader().rowId());
                deletedCount++;
                /*
                // Update indexes if applicable
                for (String columnName : indexes.keySet()) {
                    Optional<Column> columnOpt = getColumnByName(columnName);
                    if (columnOpt.isPresent()) {
                        indexes.get(columnName).delete(primaryKeyValue.toString(), row.data().get(columnOpt.get()));
                    }
                }

                 */
            }
        }
        return deletedCount;
    }

    /**
     * Creates an index on a column.
     */
    // To-do : create and use mapping between index name and column name
    public void createIndex(String indexName, String columnName) throws IOException {
        //File indexFile = new File(Constants.INDEX_DIRECTORY, tableName + "_" + columnName + Constants.INDEX_FILE_EXTENSION);
        File indexFile = new File(indexName + INDEX_FILE_EXTENSION);
        if (!indexFile.exists()) {
            if (!indexFile.createNewFile()) {
                throw new IOException("Failed to create index file for column: " + columnName);
            }
        }
        Index index = new Index(tableName, columnName, new RandomAccessFile(indexFile, "rw"));
        index.create();
        indexes.put(columnName, index);
        indexColMap.put(indexName, columnName);
        log.info("Index created for column {}.", columnName);
    }

    /**
     * Retrieves a column schema map.
     */
    private Map<String, Column> getColumnSchema() {
        Map<String, Column> schema = new HashMap<>();
        for (Column column : columns) {
            schema.put(column.name(), column);
        }
        return schema;
    }

    /**
     * Retrieves a column object by name.
     */
    private Column getColumnByName(String columnName) {
        return columns.stream()
                .filter(column -> column.name().equalsIgnoreCase(columnName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Column not found: " + columnName));
    }

}
