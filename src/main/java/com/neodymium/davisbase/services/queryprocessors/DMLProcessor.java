package com.neodymium.davisbase.services.queryprocessors;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@AllArgsConstructor
public class DMLProcessor {
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
        } else {
            System.out.println("Invalid command.");
        }
    }
}
