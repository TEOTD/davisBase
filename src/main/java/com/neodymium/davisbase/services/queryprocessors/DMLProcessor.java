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
public class DMLProcessor {
    private static final Map<String,Object> query_table = new HashMap<>();
    public void process(String query) throws IOException {
        if (query.toUpperCase().startsWith("INSERT INTO")) {
            String insertDefinition = query.substring("INSERT INTO".length()).trim();
            insertIntoTable(insertDefinition);
        } else if (query.toUpperCase().startsWith("DELETE FROM")) {
            String deleteDefinition = query.substring("DELETE FROM".length()).trim();
            deleteFromTable(deleteDefinition);
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
        String [] values = valuesDefinition.split(",");
        Table table = new Table(tableName, values); // Is this logic right for interaction with the table.java
        table.insert(query_table.put(tableName,values)); // insert the hashmap object.
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
}
