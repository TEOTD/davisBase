package com.neodymium.davisbase.services.queryprocessors;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;

@Slf4j
@Service
@AllArgsConstructor
public class DDLProcessor {

    public void processDDL(String query) throws IOException {
        if (query.startsWith("CREATE TABLE")) {
            String tableDefinition = query.substring("CREATE TABLE".length()).trim();
            createTable(tableDefinition);
        } else if (query.startsWith("DROP TABLE")) {
            String tableName = query.substring("DROP TABLE".length()).trim();
            dropTable(tableName);
        } else if (query.startsWith("CREATE INDEX")) {
            String indexDefinition = query.substring("CREATE INDEX".length()).trim();
            createIndex(indexDefinition);
        } else if (query.startsWith("DROP INDEX")) {
            String indexName = query.substring("DROP INDEX".length()).trim();
            dropIndex(indexName);
        } else if (query.startsWith("SHOW TABLES")) {
            showTables();
        } else {
            System.out.println("Invalid DDL command.");
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
        Table table = new Table(tableName, columns);
        table.create();
    }

    public void dropTable(String tableName) throws IOException {
        Table table = new Table(tableName, null);
        table.drop();
    }

    public void createIndex(String indexDefinition) throws IOException {
        String[] parts = indexDefinition.split("ON");
        if (parts.length < 2) {
            throw new IllegalArgumentException("Invalid CREATE INDEX syntax.");
        }
        String indexName = parts[0].trim();
        String tableNameAndColumn = parts[1].trim();
        String[] tableNameAndColumnParts = tableNameAndColumn.split("\\(");
        String tableName = tableNameAndColumnParts[0].trim();
        String columnName = tableNameAndColumnParts[1].replace(")", "").trim();

        Index index = new Index(indexName, tableName, columnName);
        index.create();
    }

    public void dropIndex(String indexName) throws IOException {
        Index index = new Index(indexName, null, null);
        index.drop();
    }

    public void showTables() throws IOException {
        File dir = new File("data/");
        if (!dir.exists() || dir.listFiles() == null) {
            System.out.println("No tables found.");
            return;
        }
        System.out.println("Tables:");
        for (File file : dir.listFiles()) {
            if (file.getName().endsWith(".tbl")) {
                System.out.println("- " + file.getName().replace(".tbl", ""));
            }
        }
    }

    private List<Column> parseColumns(String columnDefinition) {
        List<Column> columns = new ArrayList<>();
        String[] columnParts = columnDefinition.split(",");
        for (String part : columnParts) {
            String[] nameAndType = part.trim().split(" ");
            columns.add(new Column(nameAndType[0], nameAndType[1]));
        }
        return columns;
    }
}
