package com.neodymium.davisbase.models;

import com.neodymium.davisbase.constants.enums.PageTypes;
import com.neodymium.davisbase.error.DavisBaseException;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class BPlusTree {
    private final int pageSize;
    private final RandomAccessFile tableFile;
    private final TableCellFactory factory;

    public BPlusTree(String filePath, int pageSize, TableCellFactory factory) throws IOException {
        tableFile = new RandomAccessFile(filePath, "rw");
        this.pageSize = pageSize;
        this.factory = factory;
    }

    public void create(int pageSize) throws IOException {
        Page rootPage = new Page(pageSize, 0, PageTypes.LEAF.getValue(),
                (short) 0, (short) 0xFFFF, (short) 0xFFFF);
        writePage(rootPage, 0);
    }

    public void insert(Cell newRecord) throws IOException {
        Page rightmostPage = findRightmostLeafPage();
        try {
            rightmostPage.insert(List.of(newRecord));
            writePage(rightmostPage, rightmostPage.getPageNumber());
        } catch (DavisBaseException e) {
            handleOverflow(rightmostPage, newRecord);
        }
    }

    public void insert(List<Cell> newRecords) throws IOException {
        for (Cell record : newRecords) {
            insert(record);
        }
    }

    public void update(Cell updatedRecord) throws IOException {
        Optional<Cell> existingRecord = search(updatedRecord.rowId());
        if (existingRecord.isPresent()) {
            Page page = loadPage(existingRecord.get().pageNumber());
            page.update(List.of(updatedRecord));
            writePage(page, page.getPageNumber());
        } else {
            throw new DavisBaseException("Record not found for update.");
        }
    }

    public void update(List<Cell> updatedRecords) throws IOException {
        for (Cell updateRecord : updatedRecords) {
            update(updateRecord);
        }
    }

    public Optional<Cell> search(int rowId) throws IOException {
        Page currentPage = findRootPage();
        while (true) {
            int pageType = currentPage.getPageType();
            if (pageType == PageTypes.LEAF.getValue()) {
                return currentPage.getTableRecord(rowId)
                        .filter(Cell::exists);
            }

            if (pageType == PageTypes.INTERIOR.getValue()) {
                short childPageNo = currentPage.findChildPage(rowId);
                currentPage = loadPage(childPageNo);
                continue;
            }

            throw new DavisBaseException("Invalid page type during search.");
        }
    }


    public List<Cell> search(List<Integer> rowIds) throws IOException {
        Collections.sort(rowIds);
        int minRowId = rowIds.get(0);
        int maxRowId = rowIds.get(rowIds.size() - 1);
        List<Cell> result = new ArrayList<>();

        Page currentPage = findRootPage();

        while (currentPage.getPageType() == PageTypes.INTERIOR.getValue()) {
            short childPageNo = currentPage.findChildPage(minRowId);
            currentPage = loadPage(childPageNo);
        }

        while (currentPage.getPageType() == PageTypes.LEAF.getValue()) {
            List<Cell> allCells = currentPage.getTableCellsInRange(minRowId, maxRowId);
            for (Cell cell : allCells) {
                if (rowIds.contains(cell.rowId()) && cell.exists()) {
                    result.add(cell);
                }
            }
            if (currentPage.getMaxRowId() >= maxRowId) {
                break;
            }
            currentPage = loadPage(currentPage.getSiblingPage());
        }
        return result;
    }


    public void delete(int rowId) throws IOException {
        Optional<Cell> recordOpt = search(rowId);
        if (recordOpt.isPresent()) {
            Page page = loadPage(recordOpt.get().pageNumber());
            page.delete(List.of(recordOpt.get().rowId()));
            writePage(page, page.getPageNumber());
        }
    }

    public void delete(List<Integer> rowIds) throws IOException {
        for (int rowId : rowIds) {
            delete(rowId);
        }
    }

    public void truncate() throws IOException {
        for (long pageNo = 0; pageNo < tableFile.length() / pageSize; pageNo++) {
            Page page = loadPage((short) pageNo);
            if (page.getPageType() == PageTypes.LEAF.getValue()) {
                page.truncate();
                writePage(page, page.getPageNumber());
            }
        }
    }

    private void writePage(Page page, int pageId) throws IOException {
        tableFile.seek((long) pageId * pageSize);
        tableFile.write(page.serialize());
    }

    private Page loadPage(short pageNo) throws IOException {
        tableFile.seek((long) pageNo * pageSize);
        byte[] pageData = new byte[pageSize];
        tableFile.readFully(pageData);
        return Page.deserialize(pageData, pageNo, factory);
    }

    private Page findRootPage() throws IOException {
        short pageNo = (short) (tableFile.length() / pageSize);
        Page currentPage = loadPage(pageNo);
        return loadPage(currentPage.getRootPage());
    }

    private Page findRightmostLeafPage() throws IOException {
        Page currentPage = findRootPage();
        while (currentPage.getPageType() == PageTypes.INTERIOR.getValue()) {
            short childPageNo = currentPage.getSiblingPage();
            currentPage = loadPage(childPageNo);
        }
        return currentPage;
    }

    private void handleOverflow(Page currentPage, Cell newRecord) throws IOException {
        if (currentPage.getPageType() == PageTypes.LEAF.getValue()) {
            handleLeafOverflow(currentPage, newRecord);
        } else if (currentPage.getPageType() == PageTypes.INTERIOR.getValue()) {
            handleInteriorOverflow(currentPage, newRecord);
        } else {
            throw new DavisBaseException("Unknown page type for overflow handling");
        }
    }

    private void handleLeafOverflow(Page leafPage, Cell newRecord) throws IOException {
        int newPageId = leafPage.getPageNumber() + 1;
        Page newLeafPage = new Page(pageSize, newPageId, PageTypes.LEAF.getValue(),
                leafPage.getRootPage(), leafPage.getParentPage(), (short) 0xFFFF);
        leafPage.insert(List.of(newRecord));
        leafPage.setSiblingPage((short) newPageId);

        Cell parentRecord = factory.createParentRecord(newRecord.rowId(), leafPage.getPageNumber());
        handleParentPromotion(leafPage, newLeafPage, parentRecord);
        writePage(leafPage, leafPage.getPageNumber());
        writePage(newLeafPage, newPageId);
    }

    private void handleInteriorOverflow(Page interiorPage, Cell parentRecord) throws IOException {
        int newPageId = interiorPage.getPageNumber() + 1;
        Page newInteriorPage = new Page(pageSize, newPageId, PageTypes.INTERIOR.getValue(),
                interiorPage.getRootPage(), interiorPage.getParentPage(), interiorPage.getSiblingPage());
        interiorPage.insert(List.of(parentRecord));
        interiorPage.setSiblingPage((short) newPageId);

        Cell newParentRecord = factory.createParentRecord(parentRecord.rowId(), interiorPage.getPageNumber());
        handleParentPromotion(interiorPage, newInteriorPage, newParentRecord);
        writePage(interiorPage, interiorPage.getPageNumber());
        writePage(newInteriorPage, newPageId);
    }

    private void handleParentPromotion(Page leftPage, Page rightPage, Cell parentRecord) throws IOException {
        if (leftPage.getParentPage() == (short) 0xFFFF) {
            int newRootPageId = rightPage.getPageNumber() + 1;
            Page newRootPage = new Page(pageSize, newRootPageId, PageTypes.INTERIOR.getValue(),
                    (short) newRootPageId, (short) 0xFFFF, (short) rightPage.getPageNumber());

            newRootPage.insert(List.of(parentRecord));
            leftPage.setParentPage((short) newRootPageId);
            rightPage.setParentPage((short) newRootPageId);
            writePage(newRootPage, newRootPageId);
            updateAllPagesRootPageId(newRootPageId);
        } else {
            Page parentPage = loadPage(leftPage.getParentPage());
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
            long rootPageIdOffset = i * pageSize + 10;
            tableFile.seek(rootPageIdOffset);
            tableFile.writeShort((short) newRootPageId);
        }
    }
}
