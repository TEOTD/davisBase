package com.neodymium.davisbase.services.queryprocessors;

import com.neodymium.davisbase.constants.enums.Constraints;
import com.neodymium.davisbase.models.table.Column;
import com.neodymium.davisbase.models.table.Table;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


@Slf4j
@Service
@AllArgsConstructor
public class DMLProcessor {
    private static final Map<String,Object> query_table = new HashMap<>();
    public void process(String query) throws IOException {
        if (query.toUpperCase().startsWith("INSERT INTO")) {
            String insertDefinition = query.substring("INSERT INTO".length()).trim();
            insertIntoTable(insertDefinition);
        } else if (query.toUpperCase().startsWith("DELETE FROM")) {
            String deleteDefinition = query.substring("DELETE FROM".length()).trim();
            deleteFromTable(deleteDefinition);
        } else if (query.toUpperCase().startsWith("DROP TABLE")) {
            String tableName = query.substring("DROP TABLE".length()).trim();
            dropTable(tableName);
        } else if (query.toUpperCase().startsWith("UPDATE")) {
            String updateDefinition = query.substring("UPDATE".length()).trim();
            updateTable(updateDefinition);
        } else {
            System.out.println("Invalid command.");
        }
    }

    

    public void insertIntoTable(String insertDefinition) throws IOException {
        String[] parts = insertDefinition.split("\\s+VALUES\\s+", 2);
        if (parts.length < 2) {
            throw new IllegalArgumentException("Invalid INSERT INTO syntax.");
        }
        String tableName = parts[0].trim();
        String valuesDefinition = parts[1].replace("(", "").replace(")", "").trim();
        String [] values = valuesDefinition.split("\\s*,\\s*");

        //if columns are provided:
        String[] columns = null;
        if(tableName.contains("(")) && tableName.contains(")")){
            int indexOpenParen = tableName.indexOf('(');
            int indexCloseParen = tableName.indexOf(')');
            if(indexOpenParen < indexCloseParen){
                String columns_split = tableName.substring(indexOpenParen+1,indexCloseParen);
                columns = columns_split.split("\\s*,\\s*");
                tableName = tableName.substring(0,indexOpenParen).trim();
            }

        }
        Table table = new Table(tableName, values);
        table.insert(query_table.put(columns,values));
    }

    public void deleteFromTable(String deleteDefinition) throws IOException {
        String[] parts = deleteDefinition.split("\\s+WHERE\\s+", 2);
        String tableName = parts[0].trim();
        String condition = parts.length > 1 ? parts[1].trim() : null;
        Table.delete(condition);
    }

    public void dropTable(String tableName) throws IOException {
        Table.drop(tableName);
    }

    public void updateTable(String updateDefinition) throws IOException {
        // Split the update definition into parts
        String[] parts = updateDefinition.split("\\s+SET\\s+", 2);
        if (parts.length < 2) {
            throw new IllegalArgumentException("Invalid UPDATE syntax.");
        }

        // Extract the table name and the SET clause
        String tableName = parts[0].trim();
        String setClause = parts[1].trim();

        // Split the SET clause into individual assignments
        String[] assignments = setClause.split("\\s*,\\s*");

        // Extract the WHERE clause if it exists
        String[] whereParts = setClause.split("\\s+WHERE\\s+", 2);
        String condition = whereParts.length > 1 ? whereParts[1].trim() : null;

        // Get the table object
        Table table = new Table(tableName);

        // Process each assignment
        for (String assignment : assignments) {
            String[] keyValue = assignment.split("\\s*=\\s*", 2);
            if (keyValue.length < 2) {
                throw new IllegalArgumentException("Invalid assignment in SET clause.");
            }

            String columnName = keyValue[0].trim();
            String newValue = keyValue[1].trim();

            // Check if the column exists and if the new value is valid
            Column column = table.getColumn(columnName);
            if (column == null) {
                throw new IllegalArgumentException("Column '" + columnName + "' does not exist in table '" + tableName + "'.");
            }

            // Check constraints (e.g., NOT NULL, PRIMARY KEY)
            if (column.getConstraints().contains(Constraints.NOT_NULL) && "NULL".equalsIgnoreCase(newValue)) {
                throw new IllegalArgumentException("Column '" + columnName + "' cannot be NULL.");
            }

            if (column.getConstraints().contains(Constraints.PRIMARY_KEY) && "NULL".equalsIgnoreCase(newValue)) {
                throw new IllegalArgumentException("Primary key column '" + columnName + "' cannot be NULL.");
            }

            // Update the column value
            table.updateColumn(columnName, newValue, condition);
        }
    }
}