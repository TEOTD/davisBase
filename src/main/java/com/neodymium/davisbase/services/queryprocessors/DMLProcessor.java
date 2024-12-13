package com.neodymium.davisbase.services.queryprocessors;

import com.neodymium.davisbase.models.Table;
import com.neodymium.davisbase.models.TableRecord;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Arrays;

@Slf4j
@Service
@AllArgsConstructor
public class DMLProcessor {
    private final Table<TableRecord> table;

    public void processDML(String query) throws IOException {
        if (query.startsWith("INSERT INTO")) {
            String tableName = query.split(" ")[2];
            String values = query.substring(query.indexOf("VALUES") + 6).replace("(", "").replace(")", "").trim();
            insert(tableName, values.split(","));
        } else if (query.startsWith("DELETE FROM")) {
            String tableName = query.split(" ")[2];
            String whereClause = query.substring(query.indexOf("WHERE")).trim();
            delete(tableName, whereClause);
        } else {
            System.out.println("Invalid DML command.");
        }
    }

    public void insert(String tableName, String[] values) throws IOException {
        // Check if the number of values matches the number of columns in the table
        int numColumns = table.getColumnCount(tableName);
        if (values.length != numColumns) {
            throw new IllegalArgumentException("Number of values does not match the number of columns in table " + tableName);
        }

        // Check for NULL values in PRIMARY KEY or NOT NULL columns
        for (int i = 0; i < values.length; i++) {
            if (values[i].equalsIgnoreCase("NULL") && table.isPrimaryKey(tableName, i) || table.isNotNull(tableName, i)) {
                throw new IllegalArgumentException("NULL value not allowed for PRIMARY KEY or NOT NULL column in table " + tableName);
            }
        }

        // Generate a unique rowid
        long rowid = table.getNextRowId(tableName);

        // Insert the record into the table
        table.insert(tableName, rowid, values);
        System.out.println("Record inserted into table " + tableName + " with rowid " + rowid + ".");
    }

    public void delete(String tableName, String whereClause) throws IOException {
        // Implement the delete logic with the WHERE clause
        table.delete(tableName, whereClause);
        System.out.println("Records deleted from table " + tableName + ".");
    }
}