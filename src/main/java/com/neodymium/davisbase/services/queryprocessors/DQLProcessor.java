package com.neodymium.davisbase.services.queryprocessors;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.springframework.stereotype.Service;

@Slf4j
@Service
@AllArgsConstructor
public class DQLProcessor {
    public void processDQL(String query) throws IOException {
        if (query.startsWith("SELECT")) {
            String tableName = query.split("FROM")[1].split(" ")[1].trim();
            select(tableName);
        } else {
            System.out.println("Invalid DQL command.");
        }
    }

    public void select(String tableName) throws IOException {
        File tableFile = new File("data/" + tableName + ".tbl");
        if (!tableFile.exists()) {
            throw new IOException("Table " + tableName + " does not exist.");
        }
        try (BufferedReader reader = new BufferedReader(new FileReader(tableFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        }
    }
}
