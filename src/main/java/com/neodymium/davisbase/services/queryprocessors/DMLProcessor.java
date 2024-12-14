package com.neodymium.davisbase.services.queryprocessors;

import com.neodymium.davisbase.models.table.Table;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@AllArgsConstructor
public class DMLProcessor {
    public void process(String query) throws IOException {
        if (query.toUpperCase().startsWith("INSERT INTO")) {
            insert(query);
        } else if (query.toUpperCase().startsWith("DELETE FROM")) {
            delete(query);
        } else if (query.toUpperCase().startsWith("UPDATE")) {
            update(query);
        } else {
            System.out.println("Invalid command.");
        }
    }

    // Method to handle UPDATE queries
    private void update(String query) throws IOException {
        // Split the query to separate the SET clause from the rest of the query
        String[] parts = query.split("\\s+SET\\s+", 2);
        if (parts.length < 2) {
            throw new IllegalArgumentException("Invalid UPDATE syntax.");
        }

        // Extract the table name from the query
        String tableName = parts[0].replace("UPDATE", "").trim();
        // Extract the SET clause and the optional WHERE clause
        String setClause = parts[1];

        // Split the SET clause and the WHERE clause
        String[] setParts = setClause.split("\\s+WHERE\\s+", 2);
        String setDefinition = setParts[0].trim();
        String condition = setParts.length > 1 ? setParts[1].trim() : null;

        // Parse the SET clause into a map of column names and their new values
        Map<String, String> updateValues = parseSetClause(setDefinition);

        // Create a Table object and call its update method to perform the update
        Table table = new Table(tableName, List.of());
        table.update(updateValues, condition);
    }


    // Method to parse the SET clause into a map of column names and values
    private Map<String, String> parseSetClause(String setDefinition) {
        Map<String, String> updateValues = new HashMap<>();
        // Split the SET clause into individual assignments
        String[] assignments = setDefinition.split(",");
        for (String assignment : assignments) {
            // Split each assignment into column name and value
            String[] parts = assignment.split("=", 2);
            if (parts.length < 2) {
                throw new IllegalArgumentException("Invalid SET clause syntax.");
            }
            String columnName = parts[0].trim();
            // Remove any surrounding quotes from the value
            String value = parts[1].trim().replace("'", "").replace("\"", "");
            updateValues.put(columnName, value);
        }
        return updateValues;
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

    public void delete(String deleteDefinition) throws IOException {
        String[] parts = deleteDefinition.split("\\s+WHERE\\s+", 2);
        String tableName = parts[0].trim();
        String condition = parts.length > 1 ? parts[1].trim() : null;
        Table.delete(condition);
    }

    public void dropTable(String tableName) throws IOException {
        Table.drop(tableName);
    }

    
}
