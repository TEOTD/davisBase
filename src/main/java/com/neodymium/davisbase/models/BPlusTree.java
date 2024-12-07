package com.neodymium.davisbase.models;

import com.neodymium.davisbase.constants.enums.PageTypes;
import com.neodymium.davisbase.error.DavisBaseException;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@AllArgsConstructor
public class BPlusTree {
    private final int pageSize;
    private final RandomAccessFile tableFile;
    private final CellFactory factory;

    public void create(int pageSize) throws IOException {
        Page rootPage = new Page(pageSize, 0, PageTypes.LEAF.getValue(),
                (short) 0, (short) 0xFFFF, (short) 0xFFFF);
        writePage(rootPage, 0);
    }

    public void insert(Cell cell) throws IOException {
        Page rightmostPage = findRightmostLeafPage();
        try {
            rightmostPage.insert(List.of(cell));
            writePage(rightmostPage, rightmostPage.getPageNumber());
        } catch (DavisBaseException e) {
            handleOverflow(rightmostPage, cell);
        }
    }

    public void insert(List<Cell> cells) throws IOException {
        for (Cell cell : cells) {
            insert(cell);
        }
    }

    public void update(Cell cell) throws IOException {
        Optional<Cell> existingCell = search(cell.rowId());
        if (existingCell.isPresent()) {
            Page page = loadPage(existingCell.get().pageNumber());
            page.update(List.of(cell));
            writePage(page, page.getPageNumber());
        } else {
            throw new DavisBaseException("Cell not found for update.");
        }
    }

    public void update(List<Cell> cells) throws IOException {
        for (Cell cell : cells) {
            update(cell);
        }
    }

    public Optional<Cell> search(int rowId) throws IOException {
        Page currentPage = findRootPage();
        while (true) {
            int pageType = currentPage.getPageType();
            if (pageType == PageTypes.LEAF.getValue()) {
                return currentPage.getTableCell(rowId)
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
        Optional<Cell> optionalCell = search(rowId);
        if (optionalCell.isPresent()) {
            Page page = loadPage(optionalCell.get().pageNumber());
            page.delete(List.of(optionalCell.get().rowId()));
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

    private void handleOverflow(Page currentPage, Cell cell) throws IOException {
        if (currentPage.getPageType() == PageTypes.LEAF.getValue()) {
            handleLeafOverflow(currentPage, cell);
        } else if (currentPage.getPageType() == PageTypes.INTERIOR.getValue()) {
            handleInteriorOverflow(currentPage, cell);
        } else {
            throw new DavisBaseException("Unknown page type for overflow handling");
        }
    }

    private void handleLeafOverflow(Page leafPage, Cell cell) throws IOException {
        int newPageId = leafPage.getPageNumber() + 1;
        Page newLeafPage = new Page(pageSize, newPageId, PageTypes.LEAF.getValue(),
                leafPage.getRootPage(), leafPage.getParentPage(), (short) 0xFFFF);
        leafPage.insert(List.of(cell));
        leafPage.setSiblingPage((short) newPageId);

        Cell parentCell = factory.createParentCell(cell.rowId(), leafPage.getPageNumber());
        handleParentPromotion(leafPage, newLeafPage, parentCell);
        writePage(leafPage, leafPage.getPageNumber());
        writePage(newLeafPage, newPageId);
    }

    private void handleInteriorOverflow(Page interiorPage, Cell cell) throws IOException {
        int newPageId = interiorPage.getPageNumber() + 1;
        Page newInteriorPage = new Page(pageSize, newPageId, PageTypes.INTERIOR.getValue(),
                interiorPage.getRootPage(), interiorPage.getParentPage(), interiorPage.getSiblingPage());
        interiorPage.insert(List.of(cell));
        interiorPage.setSiblingPage((short) newPageId);

        Cell parentCell = factory.createParentCell(cell.rowId(), interiorPage.getPageNumber());
        handleParentPromotion(interiorPage, newInteriorPage, parentCell);
        writePage(interiorPage, interiorPage.getPageNumber());
        writePage(newInteriorPage, newPageId);
    }

    private void handleParentPromotion(Page leftPage, Page rightPage, Cell cell) throws IOException {
        if (leftPage.getParentPage() == (short) 0xFFFF) {
            int newRootPageId = rightPage.getPageNumber() + 1;
            Page newRootPage = new Page(pageSize, newRootPageId, PageTypes.INTERIOR.getValue(),
                    (short) newRootPageId, (short) 0xFFFF, (short) rightPage.getPageNumber());

            newRootPage.insert(List.of(cell));
            leftPage.setParentPage((short) newRootPageId);
            rightPage.setParentPage((short) newRootPageId);
            writePage(newRootPage, newRootPageId);
            updateAllPagesRootPageId(newRootPageId);
        } else {
            Page parentPage = loadPage(leftPage.getParentPage());
            try {
                parentPage.insert(List.of(cell));
                parentPage.setSiblingPage((short) rightPage.getPageNumber());
                writePage(parentPage, parentPage.getPageNumber());
            } catch (DavisBaseException e) {
                handleOverflow(parentPage, cell);
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
