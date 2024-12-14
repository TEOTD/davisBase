package com.neodymium.davisbase.services.queryprocessors;

import com.neodymium.davisbase.models.table.Table;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.neodymium.davisbase.constants.Constants.DATABASE_LINE_SEPARATOR;
import static com.neodymium.davisbase.constants.Constants.TABLE_CATALOG_NAME;

@Slf4j
@Service
@AllArgsConstructor
public class DQLProcessor {
    public void process(String query) throws IOException {
        if (query.toUpperCase().startsWith("SELECT")) {
            String selectQuery = query.substring("SELECT".length()).trim();
            select(selectQuery);
        } else if (query.toUpperCase().startsWith("SHOW")) {
            select("* from " + TABLE_CATALOG_NAME);
        } else if (query.toUpperCase().startsWith("HELP")) {
            help();
        } else {
            System.out.println("Invalid DQL command.");
        }
    }

    private void help() {
        System.out.println(DATABASE_LINE_SEPARATOR);
        System.out.println("SUPPORTED COMMANDS");
        System.out.println("All commands below are case insensitive");
        System.out.println();
        System.out.println("\tSHOW TABLES;                                                               Display all the tables in the database.");
        System.out.println("\tCREATE TABLE table_name (<column_name datatype constraint>);               Create a new table in the database.");
        System.out.println("\tINSERT INTO table_name VALUES (value1,value2,..);                          Insert a new record into the table.");
        System.out.println("\tDELETE FROM TABLE table_name WHERE column_name operator value;             Delete a record from the table whose rowid is <key_value>.");
        System.out.println("\tUPDATE table_name SET column_name = value WHERE column_name operator value Modifies the records in the table.");
        System.out.println("\tCREATE INDEX ON table_name (column_name);                                  Create index for the specified column in the table");
        System.out.println("\tSELECT * FROM table_name;                                                  Display all records in the table.");
        System.out.println("\tSELECT * FROM table_name WHERE column_name operator value;                 Display records in the table where the given condition is satisfied.");
        System.out.println("\tDROP TABLE table_name;                                                     Remove table data and its schema.");
        System.out.println("\tVERSION;                                                                   Show the program version.");
        System.out.println("\tHELP;                                                                      Show this help information.");
        System.out.println("\tEXIT;                                                                      Exit DavisBase.");
        System.out.println();
        System.out.println();
        System.out.println(DATABASE_LINE_SEPARATOR);
    }

    public void select(String query) throws IOException {
        String[] parts = query.split("FROM", 2);
        if (parts.length < 2) {
            throw new IllegalArgumentException("Invalid SELECT query syntax.");
        }

        String columnSplit = parts[0].trim();
        String tableSplit = parts[1].trim();

        List<String> columns = parseColumns(columnSplit);

        String[] tableParts = tableSplit.split("WHERE", 2);
        String tableName = tableParts[0].trim();

        String condition = tableParts.length > 1 ? tableParts[1].trim() : null;
        Table table = new Table(tableName, List.of());
        List<Map<String, Object>> records = table.select(columns, condition);
        Set<String> columnNames = records.get(0).keySet();
        for (String columnName : columnNames) {
            System.out.printf("%-15s", columnName);
        }
        System.out.println();
        System.out.println(DATABASE_LINE_SEPARATOR);
        for (Map<String, Object> record : records) {
            for (String columnName : columnNames) {
                System.out.printf("%-15s", record.get(columnName));
            }
            System.out.println();
        }
    }

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
