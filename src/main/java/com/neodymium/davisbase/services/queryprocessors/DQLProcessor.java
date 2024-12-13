package com.neodymium.davisbase.services.queryprocessors;

import com.neodymium.davisbase.constants.enums.Constraints;
import com.neodymium.davisbase.constants.enums.DataTypes;
import com.neodymium.davisbase.models.table.Column;
import com.neodymium.davisbase.models.table.Table;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Slf4j
@Service
@AllArgsConstructor


public class DQLProcessor {
    public void process(String query) throws IOException {
        if (query.toUpperCase().startsWith("SELECT")) {
            String selectQuery = query.substring("SELECT".length()).trim();
            processSelect(selectQuery);
        } else {
            System.out.println("Invalid DQL command.");
        }
    }
    // Process SELECT query and extract the relevant parts (columns, conditions)
    public void processSelect(String query) throws IOException {
        String[] parts = query.split("FROM", 2);
        if (parts.length < 2) {
            throw new IllegalArgumentException("Invalid SELECT query syntax.");
        }

        String column_split = parts[0].trim();
        String table_split = parts[1].trim();

        // Parse columns to be selected (e.g., "*", or specific columns : "column1, column2")
        List<String> columns = parseColumns(column_split);

        // Parse the table name
        String[] tableParts = table_split.split("WHERE", 2);
        String tableName = tableParts[0].trim();

        // If there's a WHERE clause, extract it
        String condition = tableParts.length > 1 ? tableParts[1].trim() : null;

        // Retrieve the table and filter records
        Table table = new Table(tableName, null);
        List<Object> records = table.select(columns, condition);

        // print them to console
        records.forEach(record -> System.out.println(record));
    }

    // Parse columns from the SELECT clause (e.g., "*", or "column1, column2")
    private List<String> parseColumns(String column_split) {
        List<String> columns = new ArrayList<>();
        if (column_split.equals("*")) {
            columns.add("*");
        } else {
            String[] columnNames = column_split.split(",");
            for (String columnName : columnNames) {
                columns.add(columnName.trim());
            }
        }
        return columns;
    }
}
