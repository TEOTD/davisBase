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

        File tableFile = new File(TABLE_DIRECTORY + tableName + TABLE_FILE_EXTENSION);
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

    public static void initializeMetadataTables() throws IOException {
        Column[] tablesSchema = {
                new Column("rowid", DataTypes.INT.getTypeCode(), Set.of(Constraints.PRIMARY_KEY)),
                new Column("table_name", DataTypes.TEXT.getTypeCode(), Set.of(Constraints.NOT_NULL)),
                new Column("record_count", DataTypes.INT.getTypeCode(), Set.of()),
                new Column("avg_length", DataTypes.SMALLINT.getTypeCode(), Set.of()),
                new Column("root_page", DataTypes.SMALLINT.getTypeCode(), Set.of())
        };

        Column[] columnsSchema = {
                new Column("rowid", DataTypes.INT.getTypeCode(), Set.of(Constraints.PRIMARY_KEY)),
                new Column("column_name", DataTypes.TEXT.getTypeCode(), Set.of(Constraints.NOT_NULL)),
                new Column("table_rowid", DataTypes.INT.getTypeCode(), Set.of(Constraints.NOT_NULL)),
                new Column("table_name", DataTypes.TEXT.getTypeCode(), Set.of(Constraints.NOT_NULL)),
                new Column("data_type", DataTypes.TEXT.getTypeCode(), Set.of(Constraints.NOT_NULL)),
                new Column("ordinal_position", DataTypes.TINYINT.getTypeCode(), Set.of(Constraints.NOT_NULL)),
                new Column("constraints", DataTypes.TEXT.getTypeCode(), Set.of()),
                new Column("index_name", DataTypes.TEXT.getTypeCode(), Set.of())
        };

        Table davisbaseTables = new Table("davisbase_tables", List.of(tablesSchema));
        Table davisbaseColumns = new Table("davisbase_columns", List.of(columnsSchema));
    }

    /**
     * Drops an index on a specified column.
     *
     * @param indexName The name of the index to drop.
     * @throws IOException If an error occurs while deleting the index.
     */
    public static void dropIndex(String indexName) throws IOException {
        // Construct the expected file name pattern
        File indexDir = new File(INDEX_DIRECTORY);
        if (!indexDir.exists() || !indexDir.isDirectory()) {
            throw new IllegalStateException("Index directory does not exist: " + INDEX_DIRECTORY);
        }

        // Find the index file matching the provided index name
        File[] matchingFiles = indexDir.listFiles((dir, name) -> name.endsWith("-" + indexName + INDEX_FILE_EXTENSION));
        if (matchingFiles == null || matchingFiles.length == 0) {
            throw new IllegalArgumentException("Index '" + indexName + "' does not exist.");
        }

        // There should only be one file matching the index name
        File indexFile = matchingFiles[0];
        if (indexFile.exists() && !indexFile.delete()) {
            throw new IOException("Failed to delete index file for index: " + indexFile.getName());
        }

        log.info("Index '{}' successfully dropped.", indexName);
    }

    /**
     * Initializes the table file with a root page.
     */
    private void initializeTableFile(File tableFile) throws IOException {
        File parentDir = tableFile.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            if (!parentDir.mkdirs()) {
                throw new IOException("Failed to create parent directories for: " + tableFile.getPath());
            }
        }
        if (tableFile.createNewFile()) {
            try (RandomAccessFile randomAccessFile = new RandomAccessFile(tableFile, "rw")) {
                BPlusTree bPlusTree = new BPlusTree(randomAccessFile);
                bPlusTree.create();
                log.info("Table file created for '{}'.", tableName);
            } catch (IOException e) {
                throw new IOException("Failed to initialize BPlusTree for: " + tableFile.getPath(), e);
            }
        } else if (!tableFile.exists()) {
            throw new IOException("Failed to create or find the table file: " + tableFile.getPath());
        }
    }

    /**
     * Initializes columns for a new table based on the given schema.
     */
    private void initializeColumnsFromSchema(List<Column> schemaInfo) {
        for (Column column : schemaInfo) {
            addColumn(column.name(), column.typeCode(), column.constraints());
        }
    }

    /**
     * Adds a column to the schema.
     */
    private void addColumn(String name, byte typeCode, Set<Constraints> constraints) {
        if (constraints.contains(Constraints.PRIMARY_KEY) && this.primaryKey != null) {
            throw new IllegalArgumentException("Table can have only one PRIMARY KEY column.");
        }

        columns.add(new Column(name, typeCode, constraints));
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
                if (bPlusTree.searchColumn(column, value).isPresent()) {
                    throw new DavisBaseException("PRIMARY KEY constraint violated: duplicate value found for column: " + column.name());
                }
            }

            // Check UNIQUE constraint
            if (column.constraints().contains(Constraints.UNIQUE)) {
                if (bPlusTree.searchColumn(column, value).isPresent()) { // Assuming `searchByColumn` checks for uniqueness
                    throw new DavisBaseException("UNIQUE constraint violated for column: " + column.name());
                }
            }

            rowData.put(column, value);
        }

        Row row = new Row(bPlusTree.getMaxRowId() + 1, rowData);
        bPlusTree.insert(Collections.singletonMap(this.primaryKey, row.cellFromRow()));

        // Update indexes
        for (Map.Entry<String, Index> entry : indexes.entrySet()) {
            String columnName = entry.getKey();
            Index index = entry.getValue();
            Object value = rowData.get(getColumnByName(columnName));
            if (value != null) {
                index.insert(value, row.id());
            }
        }

        // Update indexes
        for (Map.Entry<String, String> entry : indexColMap.entrySet()) {
            String indexName = entry.getKey(); // Get the index name
            String columnName = entry.getValue(); // Get the column name associated with the index
            Index index = indexes.get(indexName); // Retrieve the Index object using the index name

            if (index != null) {
                Object value = rowData.get(getColumnByName(columnName)); // Get the value for the column
                if (value != null) {
                    index.insert(value, row.id()); // Insert into the index
                }
            }
        }


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
                        if (!newValue.equals(row.data().get(column)) && bPlusTree.searchColumn(column, newValue).isPresent()) {
                            throw new DavisBaseException("PRIMARY KEY constraint violated: duplicate value found for column: " + column.name());
                        }
                    }

                    // Check UNIQUE constraint
                    if (column.constraints().contains(Constraints.UNIQUE)) {
                        if (!newValue.equals(row.data().get(column)) && bPlusTree.searchColumn(column, newValue).isPresent()) {
                            throw new DavisBaseException("UNIQUE constraint violated for column: " + column.name());
                        }
                    }
                    row.data().put(column, newValue);

                    // Update the index
                    if (indexColMap.containsValue(column.name())) {
                        // Retrieve the index name associated with the column name
                        String indexName = indexColMap.entrySet().stream()
                                .filter(entry -> entry.getValue().equals(column.name()))
                                .map(Map.Entry::getKey)
                                .findFirst()
                                .orElse(null);

                        if (indexName != null && indexes.containsKey(indexName)) {
                            // Update the index using the new value and the row ID
                            indexes.get(indexName).update(newValue, row.id());
                        }
                    }

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
        // Construct the index file name
        String fullIndexName = tableName + "-" + columnName + "-" + indexName;
        File indexFile = new File(INDEX_DIRECTORY + fullIndexName + INDEX_FILE_EXTENSION);
        // Check if the index file already exists
        if (indexFile.exists()) {
            throw new IllegalArgumentException("Index file '" + fullIndexName + "' already exists.");
        }

        // Create the index file
        if (!indexFile.createNewFile()) {
            throw new IOException("Failed to create index file: " + fullIndexName);
        }
        Index index = new Index(tableName, columnName, new RandomAccessFile(indexFile, "rw"));
        index.create();
        indexes.put(columnName, index);
        indexColMap.put(fullIndexName, columnName);
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

    private void addToMetadata() throws IOException {
        Table davisbaseTables = new Table("davisbase_tables", List.of());
        Table davisbaseColumns = new Table("davisbase_columns", List.of());

        // Add to davisbase_tables
        Map<String, Object> tableRecord = new HashMap<>();
        tableRecord.put("rowid", getNextRowId(davisbaseTables));
        tableRecord.put("table_name", tableName);
        tableRecord.put("record_count", 0);
        tableRecord.put("avg_length", 0);
        tableRecord.put("root_page", 0);
        davisbaseTables.insert(tableRecord);

        // Add to davisbase_columns
        int ordinalPosition = 1;
        for (Column column : columns) {
            Map<String, Object> columnRecord = new HashMap<>();
            columnRecord.put("rowid", getNextRowId(davisbaseColumns));
            columnRecord.put("column_name", column.name());
            columnRecord.put("table_rowid", tableRecord.get("rowid"));
            columnRecord.put("table_name", tableName);
            columnRecord.put("data_type", DataTypes.getFromTypeCode(column.typeCode()).getTypeName());
            columnRecord.put("ordinal_position", ordinalPosition++);
            columnRecord.put("constraints", String.join(",", column.constraints().stream()
                    .map(Constraints::getValue)
                    .toList()));
            davisbaseColumns.insert(columnRecord);
        }
    }

    private int getNextRowId(Table table) throws IOException {
        List<Map<String, Object>> results = table.select(List.of("rowid"), null);

        // If there are no rows, start with row ID 1
        if (results.isEmpty()) {
            return 1;
        }

        // Find the maximum rowid in the results
        return results.stream()
                .map(row -> (Integer) row.get("rowid"))
                .max(Integer::compare)
                .orElse(0) + 1;
    }

    private void initializeColumnsFromMetadata() throws IOException {
        Table davisbaseColumns = new Table("davisbase_columns", List.of());

        List<Map<String, Object>> columnMetadata = davisbaseColumns.select(
                List.of("column_name", "data_type", "constraints"),
                "table_name = '" + tableName + "'"
        );

        for (Map<String, Object> columnData : columnMetadata) {
            String columnName = (String) columnData.get("column_name");
            DataTypes dataType = DataTypes.getFromName((String) columnData.get("data_type"));
            Set<Constraints> constraints = parseConstraints((String) columnData.get("constraints"));

            addColumn(columnName, dataType.getTypeCode(), constraints);

        }

        // Populate indexColMap by scanning the index directory
        populateIndexColMap();
    }

    private void populateIndexColMap() {
        File indexDir = new File(INDEX_DIRECTORY);

        // Validate that the index directory exists
        if (!indexDir.exists() || !indexDir.isDirectory()) {
            log.warn("Index directory '{}' does not exist or is not a directory.", INDEX_DIRECTORY);
            return;
        }

        // Scan the directory for index files corresponding to the current table
        File[] indexFiles = indexDir.listFiles((dir, name) -> name.startsWith(tableName + "-") && name.endsWith(INDEX_FILE_EXTENSION));
        if (indexFiles == null || indexFiles.length == 0) {
            log.info("No index files found for table '{}'.", tableName);
            return;
        }

        // Populate the indexColMap
        for (File indexFile : indexFiles) {
            String fileName = indexFile.getName();
            String[] parts = fileName.replace(INDEX_FILE_EXTENSION, "").split("-");

            // Ensure the index file naming convention is correct
            if (parts.length == 3) {
                String tableNameFromFile = parts[0];
                String columnName = parts[1];
                String indexName = parts[2];

                // Check if the table name matches
                if (tableNameFromFile.equals(tableName)) {
                    indexColMap.put(indexName, columnName);
                    log.info("Index '{}' mapped to column '{}' for table '{}'.", indexName, columnName, tableName);
                }
            }
        }
    }

}
