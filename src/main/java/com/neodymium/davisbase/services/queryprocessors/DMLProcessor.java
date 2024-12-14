package com.neodymium.davisbase.services.queryprocessors;

import com.neodymium.davisbase.models.table.Table;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

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

    private void update(String query) {

    }

    public void insert(String query) throws IOException {
        String[] parts = insertDefinition.split("\\s+VALUES\\s+", 2);
        if (parts.length < 2) {
            throw new IllegalArgumentException("Invalid INSERT INTO syntax.");
        }
        String tableName = parts[0].trim();
        String valuesDefinition = parts[1].replace("(", "").replace(")", "").trim();
        String[] values = valuesDefinition.split(",");
        Table table = new Table(tableName, List.of());
        table.insert(values);
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
