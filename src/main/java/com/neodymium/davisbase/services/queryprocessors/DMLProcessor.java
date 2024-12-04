package com.neodymium.davisbase.services.queryprocessors;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.FileWriter;
import java.io.IOException;

import org.springframework.stereotype.Service;

@Slf4j
@Service
@AllArgsConstructor
public class DMLProcessor {
    public void processDML(String query) throws IOException {
        if (query.startsWith("INSERT INTO")) {
            String tableName = query.split(" ")[2];
            String values = query.substring(query.indexOf("VALUES") + 6).replace("(", "").replace(")", "").trim();
            insert(tableName, values.split(","));
        } else if (query.startsWith("DELETE FROM")) {
            System.out.println("DELETE not implemented yet.");
        } else if (query.startsWith("UPDATE")) {
            System.out.println("UPDATE not implemented yet.");
        } else {
            System.out.println("Invalid DML command.");
        }
    }

    public void insert(String tableName, String[] values) throws IOException {
        File tableFile = new File("data/" + tableName + ".tbl");
        if (!tableFile.exists()) {
            throw new IOException("Table " + tableName + " does not exist.");
        }
        try (FileWriter writer = new FileWriter(tableFile, true)) {
            writer.write(String.join(",", values) + "\n");
        }
        System.out.println("Record inserted into table " + tableName + ".");
    }
}
