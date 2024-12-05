package com.neodymium.davisbase.models;

import com.neodymium.davisbase.constants.enums.PageTypes;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.List;

public class BPlusTree<T extends TableRecord> {
    private final int pageSize;
    private final RandomAccessFile tableFile;
    private short rootPageId;

    public BPlusTree(String filePath, int pageSize) throws IOException {
        tableFile = new RandomAccessFile(filePath, "rw");
        if (tableFile.length() > 0) {
            rootPageId = 0;
        } else {
            rootPageId = -1;
        }
        this.pageSize = pageSize;
    }

    public void create(int pageSize) throws IOException {
        if (rootPageId == -1) {
            Page<T> rootPage = new Page<>(pageSize, 0, PageTypes.LEAF.getValue(), (short) -1, (short) -1, (short) -1);
            byte[] serializedRootPage = rootPage.serialize();
            tableFile.seek(0);
            tableFile.write(serializedRootPage);
            rootPageId = 0;
        }
    }

    public void insert(T tableRecord) throws IOException {
        short currentPageId = rootPageId;
        Page<T> currentPage = loadPage(currentPageId);
        while (currentPage.getPageType() != PageTypes.LEAF.getValue()) {
            currentPageId = currentPage.getSiblingPage();
            currentPage = loadPage(currentPageId);
        }
        if (currentPageIsFull(currentPage)) {
            splitPage(currentPage, currentPageId);
            currentPage = loadPage(currentPage.getSiblingPage());
        }
        currentPage.insert(List.of(tableRecord));
    }

    // Split a currentPage if it overflows
    private void splitPage(Page<T> currentPage, int currentPageNo) throws IOException {
        Page<T> rightPage = new Page<>(pageSize, PageTypes.LEAF.getValue(), currentPage.getRootPage(), (short) -1, (short) -1);
        //If currentPage is a root currentPage
        if (currentPage.getParentPage() == -1) {
            Page<T> parentPage = new Page<>(pageSize, PageTypes.INTERIOR.getValue(), currentPage.getRootPage(), (short) -1, (short) (currentPageNo + 1));
            currentPage.setSiblingPage((short) (currentPageNo + 1));
            currentPage.setParentPage();
        } else {

        }

        // Create a new sibling currentPage
        Page<T> siblingPage = new Page<>(pageSize, PageTypes.LEAF.getValue(), currentPage.getRootPage(), (short) -1, (short) -1);

        // The record causing the overflow is the first record that would exceed the currentPage's capacity
        T recordToMove = currentPage.records.get(currentPage.records.size() - 1);
        short rowIdKey = recordToMove.getRowid(); // This will be the rowid for the parent currentPage key

        // Insert the record causing the overflow into the sibling currentPage
        siblingPage.records.add(recordToMove);
        siblingPage.cellOffsets.add(currentPage.cellOffsets.get(currentPage.cellOffsets.size() - 1));

        // Remove the last record from the current currentPage (the one that overflowed)
        currentPage.records.remove(currentPage.records.size() - 1);
        currentPage.cellOffsets.remove(currentPage.cellOffsets.size() - 1);
        currentPage.numberOfCells = (short) currentPage.records.size();

        // Create or update the parent currentPage to reflect the new key
        Page<T> parentPage = loadPage(currentPage.parentPage);
        parentPage.insert(List.of(createRecord(rowIdKey)));  // The key for the parent currentPage is the rowId of the first record in the sibling currentPage

        // Update sibling currentPage link
        currentPage.siblingPage = siblingPage.rootPage;
        siblingPage.parentPage = currentPage.parentPage;

        // If the parent currentPage overflows, create a new root currentPage
        if (parentPage.numberOfCells > 1) {
            // Overflow at the root - create a new root
            short newRootPageId = createNewPage();
            rootPageId = newRootPageId;
            Page<T> newRootPage = loadPage(newRootPageId);
            newRootPage.insert(List.of(createRecord(rowIdKey)));

            // Update the new root currentPage to point to the two child pages (the original and the new sibling)
            newRootPage.siblingPage = siblingPage.rootPage;
        }
    }

    private Page<T> loadPage(short pageNo) throws IOException {
        tableFile.seek((long) pageNo * pageSize);
        byte[] pageData = new byte[pageSize];
        tableFile.read(pageData);
        return Page.deserialize(pageData, pageNo, createRecordFactory());
    }

    // Check if the page is full based on its number of cells
    private boolean currentPageIsFull(Page<T> page) {
        return page.getNumberOfCells() >= (pageSize / createRecord().getSize());
    }

    // Create a new page in the table file and return its ID
    private short createNewPage() {
        try {
            tableFile.seek(tableFile.length());
            byte[] emptyPage = new byte[pageSize];
            tableFile.write(emptyPage);
            return (short) (tableFile.length() / pageSize - 1);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Error creating new page");
        }
    }

    // Helper method to create a new record with a rowId
    private T createRecord(short rowid) {
        // Create a record with the given rowId (this assumes a constructor that takes a rowId)
        return createRecordFactory().create(rowid);
    }

    private TableRecordFactory<T> createRecordFactory() {
        return new TableRecordFactory<>() {
            @Override
            public T create(short rowId) {
                return new ExampleRecord(rowId);
            }

            @Override
            public T deserialize(byte[] data) {
                return ExampleRecord.deserialize(data);
            }
        };
    }

    // Example Record class
    public record ExampleRecord(int rowId) implements TableRecord {
        private static final int SIZE = 100; // Placeholder size

        public static ExampleRecord deserialize(byte[] data) {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            int rowId = buffer.getInt();
            return new ExampleRecord(rowId);
        }

        @Override
        public byte[] serialize() {
            ByteBuffer buffer = ByteBuffer.allocate(SIZE);
            buffer.putInt(rowId);
            return buffer.array();
        }

        @Override
        public String getPrimaryKey() {
            return "";
        }

        @Override
        public int getSize() {
            return SIZE;
        }
    }
}