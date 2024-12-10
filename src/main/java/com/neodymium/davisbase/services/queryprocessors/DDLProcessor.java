package com.neodymium.davisbase.services.queryprocessors;

import com.neodymium.davisbase.constants.enums.Constraints;
import com.neodymium.davisbase.constants.enums.DataTypes;
import com.neodymium.davisbase.models.table.Column;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
@AllArgsConstructor
public class DDLProcessor {
    public void process(String query) throws IOException {
        if (query.toUpperCase().startsWith("CREATE TABLE")) {
            String tableDefinition = query.substring("CREATE TABLE".length()).trim();
            createTable(tableDefinition);
        } else if (query.toUpperCase().startsWith("DROP TABLE")) {
            String tableName = query.substring("DROP TABLE".length()).trim();
            dropTable(tableName);
        } else if (query.toUpperCase().startsWith("CREATE INDEX")) {
            String indexDefinition = query.substring("CREATE INDEX".length()).trim();
            createIndex(indexDefinition);
        } else if (query.toUpperCase().startsWith("DROP INDEX")) {
            String indexName = query.substring("DROP INDEX".length()).trim();
            dropIndex(indexName);
        } else {
            System.out.println("Invalid command.");
        }
    }

    public void createTable(String tableDefinition) throws IOException {
        String[] parts = tableDefinition.split("\\(", 2);
        if (parts.length < 2) {
            throw new IllegalArgumentException("Invalid CREATE TABLE syntax.");
        }
        String tableName = parts[0].trim();
        String columnDefinition = parts[1].replace(")", "").trim();

        List<Column> columns = parseColumns(columnDefinition);
//        Table table = new Table(tableName);
//        table.create(columns);
    }

    public void dropTable(String tableName) throws IOException {
//        Table table = new Table(tableName);
//        table.drop();
    }

    public void createIndex(String indexDefinition) throws IOException {
        String[] parts = indexDefinition.split("(?i)\\bon\\b");
        if (parts.length < 2) {
            throw new IllegalArgumentException("Invalid CREATE INDEX syntax.");
        }
        String indexName = parts[0].trim();
        String tableNameAndColumn = parts[1].trim();
        String[] tableNameAndColumnParts = tableNameAndColumn.split("\\(");
        String tableName = tableNameAndColumnParts[0].trim();
        String columnName = tableNameAndColumnParts[1].replace(")", "").trim();

//        Table table = new Table(tableName);
//        table.createIndex(indexName, columnName);
    }

    public void dropIndex(String indexName) throws IOException {
//        Table.dropIndex(indexName);
    }

    private List<Column> parseColumns(String columnDefinition) {
        List<Column> columns = new ArrayList<>();
        String[] columnParts = columnDefinition.split(",");
        for (String part : columnParts) {
            String[] nameAndTypeConstraint = part.trim().split(" ");
            Constraints constraint = null;
            if (nameAndTypeConstraint.length > 2) {
                constraint = Constraints.getFromValue(nameAndTypeConstraint[2]);
            }
            columns.add(new Column(nameAndTypeConstraint[0], DataTypes.getFromName(nameAndTypeConstraint[1]), constraint));
        }
        return columns;
    }
}
