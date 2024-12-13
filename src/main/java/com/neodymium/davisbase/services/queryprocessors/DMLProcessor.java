package com.neodymium.davisbase.services.queryprocessors;

import com.neodymium.davisbase.models.Table;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@AllArgsConstructor
public class DMLProcessor {
    private final Table table; // Assuming Table is a service or repository

    public void process(String query) {
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
        } else if (query.toUpperCase().startsWith("INSERT INTO")) {
            String insertQuery = query.substring("INSERT INTO".length()).trim();
            insertRow(insertQuery);
        } else if (query.toUpperCase().startsWith("DELETE FROM")) {
            String deleteQuery = query.substring("DELETE FROM".length()).trim();
            deleteRow(deleteQuery);
        } else {
            System.out.println("Invalid command.");
        }
    }

    private void insertRow(String query) {
        // Assuming the query is in the format: "INSERT INTO table_name VALUES (value1, value2, ...)"
        String[] parts = query.split("\\s+", 3);
        if (parts.length < 3) {
            throw new IllegalArgumentException("Invalid INSERT INTO query");
        }

        String tableName = parts[1];
        String valuesPart = parts[2].replace("(", "").replace(")", "").trim();
        String[] values = valuesPart.split(",\\s*");

        // Call the insert method in the Table class
        table.insert(tableName, values);
    }

    private void deleteRow(String query) {
        // Assuming the query is in the format: "DELETE FROM table_name [WHERE condition]"
        String[] parts = query.split("\\s+", 3);
        if (parts.length < 2) {
            throw new IllegalArgumentException("Invalid DELETE FROM query");
        }

        String tableName = parts[1];
        String conditions = parts.length > 2 ? parts[2].trim() : "";

        // Call the delete method in the Table class
        table.delete(tableName, conditions);
    }

    // Other methods like createTable, dropTable, createIndex, dropIndex remain unchanged
}