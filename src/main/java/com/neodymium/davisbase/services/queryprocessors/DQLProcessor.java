package com.neodymium.davisbase.services.queryprocessors;

import com.neodymium.davisbase.models.Table;
import com.neodymium.davisbase.models.TableRecord;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
@Service
@AllArgsConstructor
public class DQLProcessor {
    private final Table<TableRecord> table;

    public void processDQL(String query) throws IOException {
        if (query.startsWith("SELECT")) {
            String[] parts = query.split("FROM");
            String selectClause = parts[0].substring("SELECT".length()).trim();
            String tableName = parts[1].split("WHERE")[0].trim();
            String whereClause = parts.length > 1 ? parts[1].substring(parts[1].indexOf("WHERE")).trim() : null;
            select(tableName, selectClause, whereClause);
        } else {
            System.out.println("Invalid DQL command.");
        }
    }

    public void select(String tableName, String selectClause, String whereClause) throws IOException {
        File tableFile = new File("data/" + tableName + ".tbl");
        if (!tableFile.exists()) {
            throw new IOException("Table " + tableName + " does not exist.");
        }

        // Parse the select clause to determine which columns to display
        List<String> columnsToDisplay = parseSelectClause(selectClause, tableName);

        // Read the table file and filter rows based on the WHERE clause
        List<Map<String, String>> resultSet = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(tableFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] values = line.split(",");
                Map<String, String> row = new HashMap<>();
                row.put("rowid", values[0]);
                for (int i = 1; i < values.length; i++) {
                    row.put(table.getColumnName(tableName, i - 1), values[i]);
                }

                if (whereClause == null || evaluateWhereClause(whereClause, row)) {
                    resultSet.add(row);
                }
            }
        }

        // Display the result set in a tabular format
        displayResultSet(columnsToDisplay, resultSet);
    }

    private List<String> parseSelectClause(String selectClause, String tableName) {
        List<String> columns = new ArrayList<>();
        if (selectClause.contains("*")) {
            columns.addAll(table.getColumnNames(tableName));
            columns.remove("rowid"); // Exclude rowid if * is used
        } else {
            String[] columnArray = selectClause.split(",");
            for (String column : columnArray) {
                columns.add(column.trim());
            }
        }
        return columns;
    }

    private boolean evaluateWhereClause(String whereClause, Map<String, String> row) {
        // Implement logic to evaluate the WHERE clause
        // This is a placeholder and should be implemented according to your specific requirements
        return true; // Placeholder
    }

    private void displayResultSet(List<String> columnsToDisplay, List<Map<String, String>> resultSet) {
        // Display column names
        for (String column : columnsToDisplay) {
            System.out.print(column + "\t");
        }
        System.out.println();

        // Display rows
        for (Map<String, String> row : resultSet) {
            for (String column : columnsToDisplay) {
                System.out.print(row.get(column) + "\t");
            }
            System.out.println();
        }
    }
}