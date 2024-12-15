package com.neodymium.davisbase.models.table;

import com.neodymium.davisbase.constants.enums.ConditionalOperators;
import com.neodymium.davisbase.constants.enums.Constraints;
import com.neodymium.davisbase.constants.enums.DataTypes;
import com.neodymium.davisbase.constants.enums.LogicalOperators;
import com.neodymium.davisbase.error.DavisBaseException;
import com.neodymium.davisbase.models.Cell;
import com.neodymium.davisbase.models.index.Index;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.ObjectUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.neodymium.davisbase.constants.Constants.COLUMN_CATALOG_NAME;
import static com.neodymium.davisbase.constants.Constants.INDEX_DIRECTORY;
import static com.neodymium.davisbase.constants.Constants.INDEX_FILE_EXTENSION;
import static com.neodymium.davisbase.constants.Constants.TABLE_CATALOG;
import static com.neodymium.davisbase.constants.Constants.TABLE_CATALOG_NAME;
import static com.neodymium.davisbase.constants.Constants.TABLE_DIRECTORY;
import static com.neodymium.davisbase.constants.Constants.TABLE_FILE_EXTENSION;

@Data
@Slf4j
public class Table {
    private final String tableName;
    private List<Column> columns = new ArrayList<>();
    private Map<String, Index> indexes = new HashMap<>();
    private String primaryKey;

    public Table(String tableName) throws IOException {
        this.tableName = tableName;
        if (!COLUMN_CATALOG_NAME.equals(tableName) && !TABLE_CATALOG_NAME.equals(tableName)) {
            this.columns = initializeColumns();
            this.indexes = initializeIndexes();
            this.primaryKey = columns.stream()
                    .filter(Column::isPrimaryKey)
                    .map(Column::name)
                    .findFirst()
                    .orElse(null);
        }
    }

    private Map<String, Index> initializeIndexes() throws FileNotFoundException {
        Map<String, Index> indexes = new HashMap<>();
        File indexDir = new File(INDEX_DIRECTORY);
        if (!indexDir.exists() || !indexDir.isDirectory()) {
            log.warn("Index directory '{}' does not exist or is not a directory.", INDEX_DIRECTORY);
            return indexes;
        }

        File[] indexFiles = indexDir.listFiles((dir, name) -> name.startsWith(tableName + "-") && name.endsWith(INDEX_FILE_EXTENSION));
        if (indexFiles == null || indexFiles.length == 0) {
            log.info("No index files found for table '{}'.", tableName);
            return indexes;
        }

        for (File indexFile : indexFiles) {
            String fileName = indexFile.getName();
            String[] parts = fileName.replace(INDEX_FILE_EXTENSION, "").split("-");

            if (parts.length == 3) {
                String tableNameFromFile = parts[0];
                String columnName = parts[1];
                String indexName = parts[2];

                if (tableNameFromFile.equals(tableName)) {
                    indexes.put(columnName, new Index(tableName, columnName, new RandomAccessFile(indexFile, "rw")));
                    log.info("Index '{}' mapped to column '{}' for table '{}'.", indexName, columnName, tableName);
                }
            }
        }
        return indexes;
    }

    private List<Column> initializeColumns() throws IOException {
        List<Column> columns = new ArrayList<>();
        if (exists()) {
            Table davisbaseColumns = new Table(COLUMN_CATALOG_NAME);

            Clause clause = new Clause(List.of(), List.of(new Condition(ConditionalOperators.EQUALS, "table_name", tableName)));
            List<Map<String, Object>> columnMetadata = davisbaseColumns.select(
                    List.of("column_name", "data_type", "constraints"),
                    clause
            );

            for (Map<String, Object> columnData : columnMetadata) {
                String columnName = (String) columnData.get("column_name");
                DataTypes dataType = DataTypes.getFromName((String) columnData.get("data_type"));
                Set<Constraints> constraints = parseConstraints((String) columnData.get("constraints"));
                if (constraints.contains(Constraints.PRIMARY_KEY) && this.primaryKey != null) {
                    throw new IllegalArgumentException("Table can have only one PRIMARY KEY column.");
                }
                columns.add(new Column(columnName, dataType.getTypeCode(), constraints));
            }
        }
        return columns;
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

    private RandomAccessFile getTableFileInWriteMode(String path) throws IOException {
        return new RandomAccessFile(path, "rw");
    }

    private String getTableFileName(String tableName) {
        return tableName + TABLE_FILE_EXTENSION;
    }

    private String getTableFilePath(String fileName) {
        return TABLE_DIRECTORY + File.separator + fileName;
    }

    public boolean exists() throws IOException {
        File tableFile = new File(getTableFilePath(getTableFileName(tableName)));
        return tableFile.exists() && tableFile.length() > 0;
    }

    public void create(List<Column> columns) throws IOException {
        if (exists() && !columns.isEmpty()) {
            throw new DavisBaseException("Table already exists");
        }

        BPlusTree bPlusTree = new BPlusTree(getTableFileInWriteMode(getTableFilePath(getTableFileName(tableName))));
        bPlusTree.create();

        if (!COLUMN_CATALOG_NAME.equals(tableName) && !TABLE_CATALOG_NAME.equals(tableName)) {
            Table davisbaseTables = new Table(TABLE_CATALOG_NAME);
            Table davisbaseColumns = new Table(COLUMN_CATALOG_NAME);
            Map<String, Object> tableRecord = new HashMap<>();
            tableRecord.put("rowid", getNextRowId(davisbaseTables));
            tableRecord.put("table_name", tableName);
            tableRecord.put("record_count", 0);
            tableRecord.put("avg_length", 0);
            tableRecord.put("root_page", 0);
            davisbaseTables.insert(tableRecord);

            int ordinalPosition = 1;
            for (Column column : columns) {
                Map<String, Object> columnRecord = new HashMap<>();
                columnRecord.put("rowid", getNextRowId(davisbaseColumns));
                columnRecord.put("column_name", column.name());
                columnRecord.put("table_rowid", tableRecord.get("rowid"));
                columnRecord.put("table_name", tableName);
                columnRecord.put("data_type", DataTypes.getFromTypeCode(column.typeCode()).getTypeName());
                columnRecord.put("ordinal_position", ordinalPosition++);
                if (!ObjectUtils.isEmpty(column.constraints())) {
                    columnRecord.put("constraints", String.join(",", column.constraints().stream()
                            .map(Constraints::getValue)
                            .toList()));
                } else {
                    columnRecord.put("constraints", null);
                }
                davisbaseColumns.insert(columnRecord);
            }
        }

        if (this.columns.isEmpty()) {
            this.columns.addAll(columns);
        }
    }

    private int getNextRowId(Table table) throws IOException {
        List<Map<String, Object>> results = table.select(List.of("rowid"), null);
        if (results.isEmpty()) {
            return 1;
        }
        return results.stream()
                .map(row -> (Integer) row.get("rowid"))
                .max(Integer::compare)
                .orElse(0) + 1;
    }

    private Column getColumnByName(String columnName) {
        return columns.stream()
                .filter(column -> column.name().equalsIgnoreCase(columnName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Column not found: " + columnName));
    }

    public void createIndex(String indexName, String columnName) throws IOException {
        if (!exists()) {
            throw new DavisBaseException("Table does not exists");
        }
        String fullIndexName = tableName + "-" + columnName + "-" + indexName;
        File indexFile = new File(INDEX_DIRECTORY, fullIndexName + INDEX_FILE_EXTENSION);
        if (indexFile.exists()) {
            throw new DavisBaseException("Index file '" + fullIndexName + "' already exists.");
        }
        if (!indexFile.createNewFile()) {
            throw new IOException("Failed to create index file: " + fullIndexName);
        }
        Index index = new Index(tableName, columnName, new RandomAccessFile(indexFile, "rw"));
        index.create();
        log.info("Index created for column {}.", columnName);
    }

    public void insert(Map<String, Object> values) throws IOException {
        if (!exists()) {
            throw new DavisBaseException("Table does not exists");
        }

        BPlusTree bPlusTree = new BPlusTree(getTableFileInWriteMode(getTableFilePath(getTableFileName(tableName))));
        Map<Column, Object> rowData = new HashMap<>();
        for (Column column : columns) {
            Object value = values.get(column.name());
            if (!ObjectUtils.isEmpty(column.constraints())) {
                if (value == null && column.constraints().contains(Constraints.NOT_NULL)) {
                    throw new DavisBaseException("NOT NULL constraint violated for column: " + column.name());
                }

                if (column.constraints().contains(Constraints.PRIMARY_KEY)) {
                    if (value == null) {
                        throw new DavisBaseException("PRIMARY KEY constraint violated: value cannot be null for column: " + column.name());
                    }
                    if (bPlusTree.searchColumn(column, value).isPresent()) {
                        throw new DavisBaseException("PRIMARY KEY constraint violated: duplicate value found for column: " + column.name());
                    }
                }

                if (column.constraints().contains(Constraints.UNIQUE)) {
                    if (bPlusTree.searchColumn(column, value).isPresent()) {
                        throw new DavisBaseException("UNIQUE constraint violated for column: " + column.name());
                    }
                }
            }
            rowData.put(column, value);
        }

        Row row = new Row(bPlusTree.getMaxRowId() + 1, rowData);
        bPlusTree.insert(row.cellFromRow(), this.primaryKey);

        for (Map.Entry<String, Index> entry : indexes.entrySet()) {
            String columnName = entry.getKey();
            Index index = entry.getValue();
            Object value = rowData.get(getColumnByName(columnName));
            if (value != null) {
                index.insert(value, row.id());
            }
        }

        log.info("Record inserted into table '{}'.", tableName);
    }

    public void update(Clause clause, Map<String, Object> updates) throws IOException {
        if (!exists()) {
            throw new DavisBaseException("Table does not exists");
        }

        BPlusTree bPlusTree = new BPlusTree(getTableFileInWriteMode(getTableFilePath(getTableFileName(tableName))));
        List<Cell> matchingCells = bPlusTree.search();
        for (Cell cell : matchingCells) {
            Row row = Row.fromCell(cell, columns);
            if (matchesConditions(row, clause)) {
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

                    if (indexes.containsKey(column.name())) {
                        Index index = indexes.entrySet().stream()
                                .filter(entry -> entry.getKey().equals(column.name()))
                                .map(Map.Entry::getValue)
                                .findFirst()
                                .orElse(null);

                        if (index != null) {
                            index.update(newValue, row.id());
                        }
                    }
                }
                bPlusTree.update(row.cellFromRow(), null);
            }
        }
    }

    public void delete(Clause clause) throws IOException {
        if (!exists()) {
            throw new DavisBaseException("Table does not exists");
        }

        BPlusTree bPlusTree = new BPlusTree(getTableFileInWriteMode(getTableFilePath(getTableFileName(tableName))));
        List<Cell> matchingCells = bPlusTree.search();
        for (Cell cell : matchingCells) {
            Row row = Row.fromCell(cell, columns);
            if (matchesConditions(row, clause)) {
                bPlusTree.delete(cell.cellHeader().rowId());
            }
        }
    }

    private boolean matchesConditions(Row row, Clause clause) {
        boolean result = true;

        if (ObjectUtils.isEmpty(clause)) {
            return result;
        }

        for (LogicalOperators keyword : clause.conditionKeywords()) {
            if (keyword == LogicalOperators.AND) {
                result = result && evaluateConditions(row, clause.conditions());
            } else if (keyword == LogicalOperators.OR) {
                result = result || evaluateConditions(row, clause.conditions());
            }
        }

        return result;
    }

    private boolean evaluateConditions(Row row, List<Condition> conditionList) {
        for (Condition condition : conditionList) {
            boolean matches = evaluateCondition(row, condition);
            if (!matches) {
                return false;
            }
        }
        return true;
    }

    private boolean evaluateCondition(Row row, Condition condition) {
        Object leftValue = row.data().entrySet().stream()
                .filter(entry -> entry.getKey().name().equals(condition.leftHandSideValue()))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(null);
        Object rightValue = condition.rightHandSideValue();
        return condition.operation().apply(leftValue, rightValue);
    }


    public void drop() throws IOException {
        if (!exists()) {
            throw new DavisBaseException("Table does not exists");
        }

        log.info("Dropping table '{}'.", tableName);

        File tableFile = new File(TABLE_DIRECTORY, tableName + TABLE_FILE_EXTENSION);
        if (tableFile.exists() && !tableFile.delete()) {
            throw new IOException("Failed to delete table file for table: " + tableName);
        }

        Table tableCatalog = new Table(TABLE_CATALOG);
        Clause clause = new Clause(List.of(), List.of(new Condition(ConditionalOperators.EQUALS, "table_name", tableName)));
        tableCatalog.delete(clause);

        Table columnCatalog = new Table(TABLE_CATALOG);
        columnCatalog.delete(clause);

        log.info("Table {} and its associated metadata have been successfully dropped.", tableName);
    }

    public void dropIndex(String indexName) throws IOException {
        if (!exists()) {
            throw new DavisBaseException("Table does not exists");
        }

        File indexDir = new File(INDEX_DIRECTORY);
        if (!indexDir.exists() || !indexDir.isDirectory()) {
            throw new DavisBaseException("Index directory does not exist: " + INDEX_DIRECTORY);
        }

        File[] matchingFiles = indexDir.listFiles((dir, name) -> name.endsWith(indexName + INDEX_FILE_EXTENSION));
        if (matchingFiles == null || matchingFiles.length == 0) {
            throw new DavisBaseException("Index " + indexName + " does not exist.");
        }

        File indexFile = matchingFiles[0];
        if (indexFile.exists() && !indexFile.delete()) {
            throw new DavisBaseException("Failed to delete index file for index: " + indexFile.getName());
        }
        log.info("Index {} successfully dropped.", indexName);
    }

    public List<Map<String, Object>> select(List<String> columnNames, Clause clause) throws IOException {
        if (!exists()) {
            throw new DavisBaseException("Table does not exists");
        }

        BPlusTree bPlusTree = new BPlusTree(getTableFileInWriteMode(getTableFilePath(getTableFileName(tableName))));
        List<Cell> matchingCells = bPlusTree.search();
        List<Map<String, Object>> result = new ArrayList<>();

        if (columnNames.contains("*")) {
            columnNames.remove("*");
            columnNames.addAll(columns.stream().map(Column::name).toList());
        }

        for (Cell cell : matchingCells) {
            Row row = Row.fromCell(cell, columns);
            if (matchesConditions(row, clause)) {
                Map<String, Object> selectedRow = new HashMap<>();
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
}
