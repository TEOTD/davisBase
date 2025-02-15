package com.neodymium.davisbase.models.table;

import com.neodymium.davisbase.constants.enums.PageTypes;
import com.neodymium.davisbase.error.DavisBaseException;
import com.neodymium.davisbase.models.Cell;
import com.neodymium.davisbase.models.Page;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.neodymium.davisbase.constants.Constants.PAGE_SIZE;

@AllArgsConstructor
public class BPlusTree {
    private final RandomAccessFile tableFile;

    public void create() throws IOException {
        Page rootPage = new Page((short) 0, PageTypes.LEAF,
                (short) 0, (short) 0xFFFF, (short) 0xFFFF);
        writePage(rootPage);
    }

    public void insert(Cell cell, String primaryKey) throws IOException {
        Page rightmostPage = findRightmostLeafPage();
        try {
            rightmostPage.insert(List.of(cell), primaryKey);
            writePage(rightmostPage);
        } catch (DavisBaseException e) {
            int rootPageNo = handleLeafOverflow(rightmostPage, cell, primaryKey);
            updateAllPagesRootPageId(rootPageNo);
        }
    }

    public void update(Cell cell, String primaryKey) throws IOException {
        Optional<Page> existingPage = searchPage(cell.cellHeader().rowId());
        if (existingPage.isPresent()) {
            Page page = existingPage.get();
            page.update(List.of(cell), primaryKey);
            writePage(page);
        } else {
            throw new DavisBaseException("Cell not found for update.");
        }
    }

    public void update(Map<String, Cell> cells) throws IOException {
        for (Map.Entry<String, Cell> cell : cells.entrySet()) {
            update(cell.getValue(), cell.getKey());
        }
    }

    public Optional<Cell> search(int rowId) throws IOException {
        Page currentPage = findRootPage();
        while (true) {
            PageTypes pageType = currentPage.getPageType();
            if (PageTypes.LEAF == pageType) {
                return currentPage.getCell(rowId)
                        .filter(Cell::exists);
            }

            if (PageTypes.INTERIOR == pageType) {
                short childPageNo = currentPage.findChildPage(rowId);
                currentPage = loadPage(childPageNo);
                continue;
            }

            throw new DavisBaseException("Invalid page type during search.");
        }
    }

    public Optional<Page> searchPage(int rowId) throws IOException {
        Page currentPage = findRootPage();
        while (true) {
            PageTypes pageType = currentPage.getPageType();
            if (PageTypes.LEAF == pageType) {
                if (currentPage.getCell(rowId).filter(Cell::exists).isPresent()) {
                    return Optional.of(currentPage);
                }
            } else if (PageTypes.INTERIOR == pageType) {
                short childPageNo = currentPage.findChildPage(rowId);
                currentPage = loadPage(childPageNo);
            } else {
                throw new DavisBaseException("Invalid page type encountered during search: " + pageType);
            }
        }
    }


    public List<Cell> search(List<Integer> rowIds) throws IOException {
        Collections.sort(rowIds);
        int minRowId = rowIds.get(0);
        int maxRowId = rowIds.get(rowIds.size() - 1);
        List<Cell> result = new ArrayList<>();

        Page currentPage = findRootPage();

        while (PageTypes.INTERIOR == currentPage.getPageType()) {
            short childPageNo = currentPage.findChildPage(minRowId);
            currentPage = loadPage(childPageNo);
        }

        while (PageTypes.LEAF == currentPage.getPageType()) {
            List<Cell> allCells = currentPage.getCellsInRange(minRowId, maxRowId);
            for (Cell cell : allCells) {
                if (rowIds.contains(cell.cellHeader().rowId()) && cell.exists()) {
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

    public List<Cell> search() throws IOException {
        Page currentPage = findRootPage();
        List<Cell> result = new ArrayList<>();
        while (currentPage.getPageType() == PageTypes.INTERIOR) {
            short childPageNo = currentPage.findChildPage(1);
            currentPage = loadPage(childPageNo);
        }

        while (currentPage.getPageType() == PageTypes.LEAF) {
            result.addAll(currentPage.getCells());
            short siblingPageNo = currentPage.getSiblingPage();
            if (siblingPageNo == (short) 0xFFFF) {
                break;
            }
            currentPage = loadPage(siblingPageNo);
        }
        return result;
    }

    public void delete(int rowId) throws IOException {
        Optional<Page> optionalPage = searchPage(rowId);
        if (optionalPage.isPresent()) {
            Page page = optionalPage.get();
            page.delete(List.of(rowId));
            writePage(page);
        }
    }

    public void delete(List<Integer> rowIds) throws IOException {
        for (int rowId : rowIds) {
            delete(rowId);
        }
    }

    public void truncate() throws IOException {
        for (long pageNo = 0; pageNo < tableFile.length() / PAGE_SIZE; pageNo++) {
            Page page = loadPage((short) pageNo);
            if (PageTypes.LEAF == page.getPageType()) {
                page.truncate();
                writePage(page);
            }
        }
    }

    private void writePage(Page page) throws IOException {
        tableFile.seek((long) page.getPageNumber() * PAGE_SIZE);
        tableFile.write(page.serialize());
    }

    private Page loadPage(short pageNo) throws IOException {
        tableFile.seek((long) pageNo * PAGE_SIZE);
        byte[] pageData = new byte[PAGE_SIZE];
        tableFile.readFully(pageData);
        return Page.deserialize(pageData, pageNo);
    }

    private Page findRootPage() throws IOException {
        Page currentPage = loadPage((short) 0);
        return loadPage(currentPage.getRootPage());
    }

    private Page findRightmostLeafPage() throws IOException {
        Page currentPage = findRootPage();
        while (PageTypes.INTERIOR == currentPage.getPageType()) {
            short childPageNo = currentPage.getSiblingPage();
            currentPage = loadPage(childPageNo);
        }
        return currentPage;
    }

    private int handleLeafOverflow(Page leafPage, Cell cell, String primaryKey) throws IOException {
        int newPageId = getMaxPageNo() + 1;
        Page newLeafPage = new Page((short) newPageId, PageTypes.LEAF,
                leafPage.getRootPage(), (short) 0xFFFF, (short) 0xFFFF);
        newLeafPage.insert(List.of(cell), primaryKey);
        leafPage.setSiblingPage((short) newPageId);

        Cell parentCell = TableCell.createParentCell(leafPage.getPageNumber(), cell.cellHeader().rowId());
        writePage(leafPage);
        writePage(newLeafPage);
        return handleParentPromotion(leafPage, newLeafPage, parentCell);
    }

    private int getMaxPageNo() throws IOException {
        return (int) (tableFile.length() / PAGE_SIZE) - 1;
    }

    private int handleInteriorOverflow(Page interiorPage, Page rightMostPage, Cell cell) throws IOException {
        int newPageId = getMaxPageNo() + 1;
        Page newInteriorPage = new Page((short) newPageId, PageTypes.INTERIOR,
                interiorPage.getRootPage(), (short) 0xFFFF, rightMostPage.getPageNumber());
        interiorPage.insert(List.of(cell));

        Cell parentCell = TableCell.createParentCell(interiorPage.getPageNumber(), cell.cellHeader().rowId());
        writePage(interiorPage);
        writePage(newInteriorPage);
        return handleParentPromotion(interiorPage, newInteriorPage, parentCell);
    }

    private int handleParentPromotion(Page leftPage, Page rightPage, Cell cell) throws IOException {
        if (leftPage.getParentPage() == (short) 0xFFFF) {
            int newRootPageId = getMaxPageNo() + 1;
            Page newRootPage = new Page((short) newRootPageId, PageTypes.INTERIOR,
                    (short) newRootPageId, (short) 0xFFFF, rightPage.getPageNumber());

            newRootPage.insert(List.of(cell));
            leftPage.setParentPage((short) newRootPageId);
            rightPage.setParentPage((short) newRootPageId);
            writePage(newRootPage);
            writePage(leftPage);
            writePage(rightPage);
            return newRootPageId;
        } else {
            Page parentPage = loadPage(leftPage.getParentPage());
            try {
                parentPage.insert(List.of(cell));
                parentPage.setSiblingPage(rightPage.getPageNumber());
                rightPage.setParentPage(parentPage.getPageNumber());
                writePage(parentPage);
                writePage(rightPage);
                return parentPage.getRootPage();
            } catch (DavisBaseException e) {
                return handleInteriorOverflow(parentPage, rightPage, cell);
            }
        }
    }

    private void updateAllPagesRootPageId(int newRootPageId) throws IOException {
        for (long i = 0; i < tableFile.length() / PAGE_SIZE; i++) {
            long rootPageIdOffset = (i * PAGE_SIZE) + 10;
            tableFile.seek(rootPageIdOffset);
            tableFile.writeShort((short) newRootPageId);
        }
    }

    public int getMaxRowId() throws IOException {
        Page rightmostPage = findRightmostLeafPage();
        return rightmostPage.getMaxRowId();
    }

    public Optional<Cell> searchColumn(Column column, Object value) throws IOException {
        List<Cell> cells = search();
        Row row = new Row(getMaxRowId() + 1, Map.of(column, value));
        Cell cell = row.cellFromRow();
        return cells.stream().filter(c -> Arrays.equals(c.cellPayload().serialize(), cell.cellPayload().serialize()))
                .findFirst();
    }
}
