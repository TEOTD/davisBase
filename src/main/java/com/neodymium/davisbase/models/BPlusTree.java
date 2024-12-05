package com.neodymium.davisbase.models;

import com.neodymium.davisbase.constants.enums.PageTypes;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;

public class BPlusTree<T extends TableRecord> {
    private final int pageSize;
    private final RandomAccessFile tableFile;
    private final short rootPageId;

    public BPlusTree(String filePath, int pageSize) throws IOException {
        tableFile = new RandomAccessFile(filePath, "rw");
        if (tableFile.length() > 0) {
            rootPageId = 0;
        } else {
            rootPageId = -1;
        }
        this.pageSize = pageSize;
    }

    private void writePage(Page<T> page, short pageId) throws IOException {
        long position = (long) pageId * pageSize;
        tableFile.seek(position);
        tableFile.write(page.serialize());
    }

    public void insert(T tableRecord, TableRecordFactory<T> factory) throws IOException {
        short currentPageId = rootPageId;
        Page<T> currentPage = loadPage(currentPageId, factory);
        while (currentPage.getPageType() != PageTypes.LEAF.getValue()) {
            currentPageId = currentPage.getSiblingPage();
            currentPage = loadPage(currentPageId, factory);
        }
        if (currentPageIsFull(currentPage, tableRecord.getSize())) {
            splitPage(currentPage, currentPageId);
            currentPage = loadPage(currentPage.getSiblingPage(), factory);
        }
        currentPage.insert(List.of(tableRecord));
        writePage(currentPage, currentPageId);
    }

    private void splitPage(Page<T> currentPage, int currentPageNo) throws IOException {
        //split page according to b+ one tree.
        //if
    }

    private Page<T> loadPage(short pageNo, TableRecordFactory<T> factory) throws IOException {
        tableFile.seek((long) pageNo * pageSize);
        byte[] pageData = new byte[pageSize];
        tableFile.read(pageData);
        return Page.deserialize(pageData, pageNo, factory);
    }

    private boolean currentPageIsFull(Page<T> page, int size) {
        return page.getNumberOfCells() >= (pageSize / size);
    }
}