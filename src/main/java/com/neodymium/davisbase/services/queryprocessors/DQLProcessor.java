package com.neodymium.davisbase.services.queryprocessors;

import com.neodymium.davisbase.models.Table;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
@AllArgsConstructor
public class DQLProcessor {
    private final Table table; // Assuming Table is a service or repository

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

        // Parse the SELECT part
        String[] parts = query.split("(?i)\\bFROM\\b", 2);
        if (parts.length < 2) {
            throw new IllegalArgumentException("Invalid SELECT query: missing 'FROM'");
        }

        String selectPart = parts[0].replaceFirst("(?i)^SELECT\\s+", "").trim();
        String[] columnNames = parseColumnNames(selectPart);

        // Parse the FROM part
        String fromPart = parts[1].trim();
        String tableName;
        String conditions = "";

        if (fromPart.toLowerCase().contains("where")) {
            String[] tableAndConditions = fromPart.split("(?i)\\bWHERE\\b", 2);
            tableName = tableAndConditions[0].trim();
            conditions = tableAndConditions[1].trim();
        } else {
            tableName = fromPart.trim();
        }

        // Fetch the data from the table
        List<Map<String, Object>> rows = table.select(tableName, conditions);

        // Display the result set
        displayResult(columnNames, rows);
    }

    private String[] parseColumnNames(String selectPart) {
        if (selectPart.equalsIgnoreCase("*")) {
            return table.getColumnNames(false); // Exclude rowid
        } else {
            return Arrays.stream(selectPart.split(",\\s*"))
                    .map(String::trim)
                    .toArray(String[]::new);
        }
    }

    private void displayResult(String[] columnNames, List<Map<String, Object>> rows) {
        // Calculate the maximum width for each column
        Map<String, Integer> columnWidths = new HashMap<>();
        for (String columnName : columnNames) {
            columnWidths.put(columnName, columnName.length());
        }

        for (Map<String, Object> row : rows) {
            for (String columnName : columnNames) {
                Object value = row.get(columnName);
                int length = value != null ? value.toString().length() : 4; // "NULL"
                columnWidths.put(columnName, Math.max(columnWidths.get(columnName), length));
            }
        }

        // Print the header
        printRow(columnNames, columnWidths);

        // Print the separator
        printSeparator(columnNames, columnWidths);

        // Print the rows
        for (Map<String, Object> row : rows) {
            String[] values = Arrays.stream(columnNames)
                    .map(columnName -> row.get(columnName) != null ? row.get(columnName).toString() : "NULL")
                    .toArray(String[]::new);
            printRow(values, columnWidths);
        }
    }

    private void printRow(String[] values, Map<String, Integer> columnWidths) {
        for (String value : values) {
            System.out.printf("| %-" + columnWidths.get(value) + "s ", value);
        }
        System.out.println("|");
    }

    private void printSeparator(String[] columnNames, Map<String, Integer> columnWidths) {
        for (String columnName : columnNames) {
            System.out.print("+");
            for (int i = 0; i < columnWidths.get(columnName) + 2; i++) {
                System.out.print("-");
            }
        }
        System.out.println("+");
    }

    public void showTables() throws IOException {
        select("SELECT * FROM davisbase_tables");
    }
}