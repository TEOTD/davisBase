package com.neodymium.davisbase.models;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;

import static com.neodymium.davisbase.constants.Constants.COLUMN_CATALOG_FILE_NAME;
import static com.neodymium.davisbase.constants.Constants.TABLE_CATALOG_FILE_NAME;
import static com.neodymium.davisbase.constants.Constants.TABLE_FILE_EXTENSION;

public record Table(String filePath, String tableDir, String catalogDir) {
    private RandomAccessFile getTableFileInWriteMode(String path) throws IOException {
        return new RandomAccessFile(path, "rw");
    }

    private RandomAccessFile getTableFileInReadMode(String path) throws IOException {
        return new RandomAccessFile(path, "r");
    }

    private String getTableFileName(String tableName) {
        return tableName + TABLE_FILE_EXTENSION;
    }

    public boolean exists(String tableName) {
        //todo: check if table exists in table catalog
        //todo: check if table records are present in column catalog
        //todo: check if table file exists
        //todo: If all three are true then return table exists else false.
        return false;
    }

    public void create(String tableName, List<Column> columns) {
        //todo: if file does not exist initialize the file
    }

    public void insert(String tableName, List<String> values) throws IOException {
        insert(tableName, values, tableDir);
    }

    public void insert(String tableName, List<String> values, String directory) throws IOException {
    }

    public void update(String tableName, List<String> set, List<String> condition) throws IOException {
        update(tableName, set, condition, tableDir);
    }

    public void update(String tableName, List<String> set, List<String> condition, String directory) throws IOException {

    }

    public void drop(String tableName) throws IOException {
    }

    public void delete(String tableName, List<String> condition) throws IOException {
        delete(tableName, condition, tableDir);
    }

    public void delete(String tableName, List<String> condition, String directory) throws IOException {
    }

    public TableRecord select(String tableName, List<String> columns, List<String> condition) throws IOException {
        if (tableName.equalsIgnoreCase(TABLE_CATALOG_FILE_NAME) || tableName.equalsIgnoreCase(COLUMN_CATALOG_FILE_NAME)) {
            return select(tableName, columns, condition, catalogDir);
        }
        return select(tableName, columns, condition, tableDir);
    }

    public TableRecord select(String tableName, List<String> columns, List<String> condition, String directory) throws IOException {
        return null;
    }
}