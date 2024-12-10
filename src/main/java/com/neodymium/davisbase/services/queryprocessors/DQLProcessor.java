package com.neodymium.davisbase.services.queryprocessors;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Slf4j
@Service
@AllArgsConstructor
public class DQLProcessor {
    public void process(String query) throws IOException {
        if (query.toUpperCase().startsWith("SELECT")) {
            select(query);
        } else if (query.toUpperCase().startsWith("SHOW TABLES")) {
            showTables();
        } else {
            System.out.println("Invalid command.");
        }
    }

    public void select(String query) {
        if (query == null || query.trim().isEmpty()) {
            throw new IllegalArgumentException("Query cannot be null or empty");
        }
        query = query.trim();
        String[] parts = query.split("(?i)\\bFROM\\b", 2);
        if (parts.length < 2) {
            throw new IllegalArgumentException("Invalid SELECT query: missing 'FROM'");
        }

        String[] columnNames = parts[0].replaceFirst("(?i)^SELECT\\s+", "").trim().split(",\\s*");
        String tableName;
        String conditions = "";

        if (parts[1].toLowerCase().contains("where")) {
            String[] tableAndConditions = parts[1].split("(?i)\\bWHERE\\b", 2);
            tableName = tableAndConditions[0].trim();
            conditions = tableAndConditions[1].trim();
        } else {
            tableName = parts[1].trim();
        }
    }


    public void showTables() throws IOException {
        select("select * from davisbase_tables");
    }
}
