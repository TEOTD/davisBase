package com.neodymium.davisbase.models.table;

import com.neodymium.davisbase.error.DavisBaseException;
import com.neodymium.davisbase.models.Cell;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.neodymium.davisbase.constants.Constants.COLUMN_CATALOG;
import static com.neodymium.davisbase.constants.Constants.TABLE_CATALOG;
import static com.neodymium.davisbase.constants.Constants.TABLE_DIRECTORY;
import static com.neodymium.davisbase.constants.Constants.TABLE_FILE_EXTENSION;


public record Table(String name) {
    public String getFilePath() {
        return TABLE_DIRECTORY + File.separator + name + TABLE_FILE_EXTENSION;
    }

    public boolean exists() {
        try {
            if (!isEntryPresent(TABLE_CATALOG, 0)) {
                return false;
            }
            if (!areAllEntriesValid(COLUMN_CATALOG, List.of(0))) {
                return false;
            }
            return isFileNonEmpty(getFilePath());
        } catch (Exception e) {
            return false;
        }
    }

    private boolean isEntryPresent(String catalogPath, int rowId) {
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(catalogPath, "r")) {
            BPlusTree bPlusTree = new BPlusTree(randomAccessFile);
            Optional<Cell> optionalCell = bPlusTree.search(rowId);
            return optionalCell.isPresent() && optionalCell.get().exists();
        } catch (Exception e) {
            return false;
        }
    }

    private boolean areAllEntriesValid(String catalogPath, List<Integer> columnRowIds) {
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(catalogPath, "r")) {
            BPlusTree bPlusTree = new BPlusTree(randomAccessFile);
            List<Cell> cells = bPlusTree.search(columnRowIds);
            return !cells.isEmpty() && cells.stream().allMatch(cell -> cell.record());
        } catch (Exception e) {
            return false;
        }
    }

    private boolean isFileNonEmpty(String filePath) {
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(filePath, "r")) {
            return randomAccessFile.length() > 0;
        } catch (IOException e) {
            return false;
        }
    }

    public void create(List<Column> columns) throws IOException {
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(getFilePath(), "rw")) {
            BPlusTree bPlusTree = new BPlusTree(randomAccessFile);
            bPlusTree.create();
        } catch (IOException exception) {
            throw new DavisBaseException(exception);
        }

        List<Row> davisBaseTableRows = new ArrayList<>();
        List<Row> davisBaseColumRows = new ArrayList<>();
        insert(davisBaseTableRows, TABLE_CATALOG);
        insert(davisBaseColumRows, COLUMN_CATALOG);
    }

    public void insert(List<Row> values) throws IOException {
        insert(values, getFilePath());
    }

    public void insert(List<Row> values, String filePath) throws IOException {
    }

    public void update(List<String> set, List<String> condition) throws IOException {
        update(set, condition, getFilePath());
    }

    public void update(List<String> set, List<String> condition, String filePath) throws IOException {

    }

    public void drop() throws IOException {
    }

    public void delete(List<String> condition) throws IOException {
        delete(condition, getFilePath());
    }

    public void delete(List<String> condition, String filePath) throws IOException {
    }

    public List<Cell> select(List<String> columns, List<String> condition) throws IOException {
        if () {
            String tableName = "";
            String catalogDir = "";
            return select(tableName, columns, condition, catalogDir);
        }
        return select(name, columns, condition, getFilePath());
    }

    public List<Cell> select(String tableName, List<String> columns, List<String> condition, String filePath) throws IOException {
        return null;
    }
}