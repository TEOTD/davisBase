package com.neodymium.davisbase.models;

import com.neodymium.davisbase.constants.enums.PageTypes;
import com.neodymium.davisbase.error.DavisBaseException;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;

public class BPlusTree<T extends TableRecord> {
    private final int pageSize;
    private final RandomAccessFile tableFile;
    private final TableRecordFactory<T> factory;

    public BPlusTree(String filePath, int pageSize, TableRecordFactory<T> factory) throws IOException {
        tableFile = new RandomAccessFile(filePath, "rw");
        this.pageSize = pageSize;
        this.factory = factory;
    }

    private void writePage(Page<T> page, int pageId) throws IOException {
        tableFile.seek((long) pageId * pageSize);
        tableFile.write(page.serialize());
    }

    private Page<T> loadPage(short pageNo) throws IOException {
        tableFile.seek((long) pageNo * pageSize);
        byte[] pageData = new byte[pageSize];
        tableFile.readFully(pageData);
        return Page.deserialize(pageData, pageNo, factory);
    }

    public void insert(T newRecord) throws IOException {
        Page<T> rightmostPage = findRightmostLeafPage();
        try {
            rightmostPage.insert(List.of(newRecord));
            writePage(rightmostPage, rightmostPage.getPageNumber());
        } catch (DavisBaseException e) {
            handleOverflow(rightmostPage, newRecord);
        }
    }

    private Page<T> findRightmostLeafPage() throws IOException {
        short pageNo = 0; // Start at the root page
        Page<T> currentPage = loadPage(pageNo);
        while (currentPage.getPageType() == PageTypes.INTERIOR.getValue()) {
            short childPageNo = currentPage.getSiblingPage();
            currentPage = loadPage(childPageNo);
        }
        return currentPage;
    }

    private void handleOverflow(Page<T> currentPage, T newRecord) throws IOException {
        if (currentPage.getPageType() == PageTypes.LEAF.getValue()) {
            handleLeafOverflow(currentPage, newRecord);
        } else if (currentPage.getPageType() == PageTypes.INTERIOR.getValue()) {
            handleInteriorOverflow(currentPage, newRecord);
        } else {
            throw new DavisBaseException("Unknown page type for overflow handling");
        }
    }

    private void handleLeafOverflow(Page<T> leafPage, T newRecord) throws IOException {
        int newPageId = leafPage.getPageNumber() + 1;
        Page<T> newLeafPage = new Page<>(pageSize, newPageId, PageTypes.LEAF.getValue(),
                leafPage.getRootPage(), leafPage.getParentPage(), (short) 0xFFFF);
        leafPage.insert(List.of(newRecord));
        leafPage.setSiblingPage((short) newPageId);

        T parentRecord = factory.createParentRecord(newRecord.getRowId(), leafPage.getPageNumber());
        handleParentPromotion(leafPage, newLeafPage, parentRecord);
        writePage(leafPage, leafPage.getPageNumber());
        writePage(newLeafPage, newPageId);
    }

    private void handleInteriorOverflow(Page<T> interiorPage, T parentRecord) throws IOException {
        int newPageId = interiorPage.getPageNumber() + 1;
        Page<T> newInteriorPage = new Page<>(pageSize, newPageId, PageTypes.INTERIOR.getValue(),
                interiorPage.getRootPage(), interiorPage.getParentPage(), interiorPage.getSiblingPage());
        interiorPage.insert(List.of(parentRecord));
        interiorPage.setSiblingPage((short) newPageId);

        T newParentRecord = factory.createParentRecord(parentRecord.getRowId(), interiorPage.getPageNumber());
        handleParentPromotion(interiorPage, newInteriorPage, newParentRecord);
        writePage(interiorPage, interiorPage.getPageNumber());
        writePage(newInteriorPage, newPageId);
    }

    private void handleParentPromotion(Page<T> leftPage, Page<T> rightPage, T parentRecord) throws IOException {
        if (leftPage.getParentPage() == (short) 0xFFFF) {
            int newRootPageId = rightPage.getPageNumber() + 1;
            Page<T> newRootPage = new Page<>(pageSize, newRootPageId, PageTypes.INTERIOR.getValue(),
                    (short) newRootPageId, (short) 0xFFFF, (short) rightPage.getPageNumber());

            newRootPage.insert(List.of(parentRecord));
            leftPage.setParentPage((short) newRootPageId);
            rightPage.setParentPage((short) newRootPageId);
            writePage(newRootPage, newRootPageId);
            updateAllPagesRootPageId(newRootPageId);
        } else {
            Page<T> parentPage = loadPage(leftPage.getParentPage());
            try {
                parentPage.insert(List.of(parentRecord));
                parentPage.setSiblingPage((short) rightPage.getPageNumber());
                writePage(parentPage, parentPage.getPageNumber());
            } catch (DavisBaseException e) {
                handleOverflow(parentPage, parentRecord);
            }
        }
    }

    private void updateAllPagesRootPageId(int newRootPageId) throws IOException {
        for (long i = 0; i < tableFile.length() / pageSize; i++) {
            Page<T> page = loadPage((short) i);
            page.setRootPage((short) newRootPageId);
            long rootPageIdOffset = i * pageSize + 10;
            tableFile.seek(rootPageIdOffset);
            tableFile.writeShort((short) newRootPageId);
        }
    }
}
