package com.neodymium.davisbase.services.queryprocessors;
import java.io.*;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@AllArgsConstructor
public class DDLProcessor {
    public void processDDL(String query) throws IOException {
        if (query.startsWith("CREATE TABLE")) {
            String tableDefinition = query.substring("CREATE TABLE".length()).trim();
            createTable(tableDefinition);
        } else if (query.startsWith("DROP TABLE")) {
            String tableName = query.substring("DROP TABLE".length()).trim();
            drop(tableName);
        } else if (query.startsWith("SHOW TABLES")) {
            showTables();
        } else {
            System.out.println("Invalid DDL command.");
        }
    }

    public void createTable(String tableDefinition) throws IOException {
        String[] parts = tableDefinition.split("\\(", 2);
        if (parts.length < 2) {
            throw new IllegalArgumentException("Invalid CREATE TABLE syntax.");
        }
        String tableName = parts[0].trim();
        String columnDefinition = parts[1].replace(")", "").trim();

        File tableFile = new File("data/" + tableName + ".tbl");
        if (tableFile.exists()) {
            System.out.println("Table " + tableName + " already exists.");
            return;
        }

        tableFile.getParentFile().mkdirs();
        tableFile.createNewFile();
        System.out.println("Table " + tableName + " created successfully.");
    }

    public void drop(String tableName) throws IOException {
        File tableFile = new File("data/" + tableName + ".tbl");
        if (!tableFile.exists()) {
            System.out.println("Table " + tableName + " does not exist.");
            return;
        }
        if (tableFile.delete()) {
            System.out.println("Table " + tableName + " dropped successfully.");
        } else {
            throw new IOException("Failed to delete table file.");
        }
    }

    public void showTables() throws IOException {
        File dir = new File("data/");
        if (!dir.exists() || dir.listFiles() == null) {
            System.out.println("No tables found.");
            return;
        }
        System.out.println("Tables:");
        for (File file : dir.listFiles()) {
            if (file.getName().endsWith(".tbl")) {
                System.out.println("- " + file.getName().replace(".tbl", ""));
            }
        }
    }
}
