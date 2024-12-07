package com.neodymium.davisbase.models;

import com.neodymium.davisbase.error.DavisBaseException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

public class Table {
    public static final int PAGE_SIZE = 512;
    private final static String TABLES_META_FILE = "data/davisbase_tables.tbl";
    private final static String COLUMNS_META_FILE = "data/davisbase_columns.tbl";
    private final String tableName;
    private final String tableFilePath;
    private final String indexFilePath;
    private final BPlusTree bPlusTree;
    private final BTree<Integer> primaryKeyIndex;

    public Table(String tableName) throws IOException {
        this.tableName = tableName;
        this.tableFilePath = "data/" + tableName + ".tbl";
        this.indexFilePath = "data/" + tableName + "_index.ndx";

        File tableFile = new File(tableFilePath);
        File indexFile = new File(indexFilePath);

        if (!tableFile.exists()) {
            initializeTableFile(tableFile);
        }

        if (!indexFile.exists()) {
            initializeIndexFile(indexFile);
        }

        this.bPlusTree = new BPlusTree(tableFilePath, PAGE_SIZE);
        this.primaryKeyIndex = new BPlusTree(indexFilePath, PAGE_SIZE);
    }

    /**
     * Initializes a new table file with an empty root page and updates metadata files.
     */

    private void initializeTableFile(File tableFile) throws IOException {
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(tableFile, "rw")) {
            randomAccessFile.setLength(PAGE_SIZE);
            byte[] emptyPage = new byte[PAGE_SIZE];
            randomAccessFile.write(emptyPage);
        }
        registerTableInMetaData();
    }

    /**
     * Initializes a new index file with an empty root page.
     */
    private void initializeIndexFile(File indexFile) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(indexFile, "rw")) {
            raf.setLength(PAGE_SIZE);
            raf.seek(0);
            raf.writeByte(0x05); // Internal page type
            raf.writeShort(0);   // Number of keys
            raf.writeShort(PAGE_SIZE); // Starting position of free space
            raf.writeInt(0xFFFFFFFF); // No right sibling
        }
    }

    /**
     * Registers the table in the `davisbase_tables.tbl` and `davisbase_columns.tbl` metadata files.
     */
    private void registerTableInMetaData() throws IOException {
        try (RandomAccessFile tablesMeta = new RandomAccessFile(TABLES_META_FILE, "rw")) {
            int rowId = getNextRowId(tablesMeta);

            String[] values = {String.valueOf(rowId), tableName};
            insertIntoMetaTable(tablesMeta, TABLES_META_FILE, values);
        }
    }

    /**
     * Retrieves the next available row ID in the metadata file.
     */
    private int getNextRowId(RandomAccessFile file) throws IOException {
        int numPages = (int) (file.length() / PAGE_SIZE);
        for (int page = numPages; page >= 1; page--) {
            file.seek((long) (page - 1) * PAGE_SIZE);
            if (file.readByte() == 0x0D) {
                int[] keys = BPlusTree.getKeyArray(file, page);
                return keys[keys.length - 1] + 1;
            }
        }
        return 1;
    }

    /**
     * Inserts a record into the table and updates the primary key index.
     */
    public void insert(TableCell record) throws IOException {
        if (!bPlusTree.insert(record)) {
            throw new DavisBaseException("Failed to insert record into table.");
        }

        // Update the primary key index
        int primaryKey = Integer.parseInt(record.primaryKey());
        primaryKeyIndex.insert(primaryKey, record.rowId());
    }

    /**
     * Retrieves records matching the given condition.
     */
    public List<TableCell> select(String condition) throws IOException {
        return bPlusTree.search(condition);
    }

    /**
     * Deletes records matching the given condition.
     */
    public void delete(String condition) throws IOException {
        List<TableCell> records = bPlusTree.search(condition);
        for (TableCell record : records) {
            bPlusTree.delete(record.rowId());

            // Remove the primary key from the index
            int primaryKey = Integer.parseInt(record.primaryKey());
            primaryKeyIndex.delete(primaryKey);
        }
    }

    /**
     * Inserts metadata into a specified metadata table.
     */
    private void insertIntoMetaTable(RandomAccessFile file, String tableName, String[] values) throws IOException {
        String[] dataTypes = {"INT", "TEXT"};
        byte[] dataTypeCodes = {0x06, (byte) (0x0C + values[1].length())};
        int payloadSize = calculatePayloadSize(dataTypes, values);

        int page = BPlusTree.searchKeyPage(file, Integer.parseInt(values[0]));
        if (page == 0) page = 1;

        int offset = BPlusTree.checkLeafSpace(file, page, payloadSize);
        if (offset != -1) {
            BPlusTree.insertLeafCell(file, page, offset, payloadSize, Integer.parseInt(values[0]), dataTypeCodes, values);
        } else {
            BPlusTree.splitLeaf(file, page);
            insertIntoMetaTable(file, tableName, values);
        }
    }

    /**
     * Calculates the payload size for the given data types and values.
     */
    private int calculatePayloadSize(String[] dataTypes, String[] values) {
        int size = dataTypes.length;
        for (int i = 0; i < dataTypes.length; i++) {
            size += BPlusTree.calculateFieldLength(dataTypes[i], values[i]);
        }
        return size;
    }

    /**
     * Retrieves the schema of a table from `davisbase_columns.tbl`.
     */
    public List<String[]> getTableSchema() throws IOException {
        List<String[]> schema = new ArrayList<>();
        try (RandomAccessFile columnsMeta = new RandomAccessFile(COLUMNS_META_FILE, "rw")) {
            int numPages = (int) (columnsMeta.length() / PAGE_SIZE);
            for (int page = 1; page <= numPages; page++) {
                columnsMeta.seek((long) (page - 1) * PAGE_SIZE);
                if (columnsMeta.readByte() == 0x0D) {
                    int numCells = BPlusTree.getCellNumber(columnsMeta, page);
                    for (int i = 0; i < numCells; i++) {
                        long cellLocation = BPlusTree.getCellLoc(columnsMeta, page, i);
                        String[] values = BPlusTree.retrieveValues(columnsMeta, cellLocation);
                        if (values[1].equals(tableName)) {
                            schema.add(values);
                        }
                    }
                }
            }
        }
        return schema;
    }
}