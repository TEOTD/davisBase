package com.neodymium.davisbase.models.index;

import com.neodymium.davisbase.constants.enums.PageTypes;
import com.neodymium.davisbase.error.DavisBaseException;
import com.neodymium.davisbase.models.Cell;
import com.neodymium.davisbase.models.Page;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static com.neodymium.davisbase.constants.Constants.PAGE_SIZE;

@AllArgsConstructor
public class BTree {
    private final RandomAccessFile indexFile;

    public void create() throws IOException {
        Page rootPage = new Page(PAGE_SIZE, (short) 0, PageTypes.LEAF_INDEX,
                (short) 0, (short) 0xFFFF, (short) 0xFFFF);
        writePage(rootPage, 0);
    }

    public void insert(Cell cell) throws IOException {
        Page leafPage = findLeafPageForInsert(cell.cellPayload().getKey());
        try {
            leafPage.insert(List.of(cell));
            writePage(leafPage, leafPage.getPageNumber());
        } catch (DavisBaseException e) {
            handleOverflow(leafPage, cell);
        }
    }

    public void update(Cell cell) throws IOException {
        Optional<Page> pageOptional = searchPage(cell.cellPayload().getKey());
        if (pageOptional.isEmpty()) {
            throw new DavisBaseException("Cell not found for update");
        }
        Page page = pageOptional.get();
        page.update(List.of(cell));
        writePage(page, page.getPageNumber());
    }

    public Optional<Cell> search(Object key) throws IOException {
        Page currentPage = findRootPage();
        while (true) {
            Optional<Cell> cell = currentPage.getCell(key);
            if (cell.isPresent()) {
                return cell;
            }
            if (currentPage.getPageType() == PageTypes.LEAF_INDEX) {
                break;
            }
            currentPage = loadPage(currentPage.findChildPage(key));
        }
        return Optional.empty();
    }

    public Optional<Page> searchPage(Object key) throws IOException {
        Page currentPage = findRootPage();
        while (true) {
            Optional<Cell> cell = currentPage.getCell(key);
            if (cell.isPresent()) {
                return Optional.of(currentPage);
            }
            if (currentPage.getPageType() == PageTypes.LEAF_INDEX) {
                break;
            }
            currentPage = loadPage(currentPage.findChildPage(key));
        }
        return Optional.empty();
    }

    public List<Cell> search() throws IOException {
        List<Cell> results = new ArrayList<>();
        Page currentPage = findRootPage();
        while (true) {
            results.addAll(currentPage.getCells());
            if (currentPage.getPageType() == PageTypes.LEAF_INDEX) {
                break;
            }
            currentPage = loadPage(currentPage.getSiblingPage());
        }
        return results;
    }

    private Page findLeafPageForInsert(Object key) throws IOException {
        Page currentPage = findRootPage();
        while (currentPage.getPageType() != PageTypes.LEAF_INDEX) {
            currentPage = loadPage(currentPage.findChildPage(key));
        }
        return currentPage;
    }

    private void writePage(Page page, int pageNo) throws IOException {
        indexFile.seek((long) pageNo * PAGE_SIZE);
        indexFile.write(page.serialize());
    }

    private Page loadPage(short pageNo) throws IOException {
        indexFile.seek((long) pageNo * PAGE_SIZE);
        byte[] pageData = new byte[PAGE_SIZE];
        indexFile.readFully(pageData);
        return Page.deserialize(pageData, pageNo);
    }

    private Page findRootPage() throws IOException {
        short pageNo = (short) (indexFile.length() / PAGE_SIZE);
        Page currentPage = loadPage(pageNo);
        return loadPage(currentPage.getRootPage());
    }

    private void handleOverflow(Page currentPage, Cell cell) throws IOException {
        if (PageTypes.LEAF_INDEX == currentPage.getPageType()) {
            handleLeafOverflow(currentPage, cell);
        } else if (PageTypes.INTERIOR_INDEX == currentPage.getPageType()) {
            handleInteriorOverflow(currentPage, cell);
        } else {
            throw new DavisBaseException("Unknown page type for overflow handling");
        }
    }

    private void handleLeafOverflow(Page leafPage, Cell cell) throws IOException {
        int newPageId = leafPage.getPageNumber() + 1;
        Page newLeafPage = new Page(PAGE_SIZE, (short) newPageId, PageTypes.LEAF_INDEX,
                leafPage.getRootPage(), leafPage.getParentPage(), (short) 0xFFFF);

        List<Cell> allCells = new ArrayList<>(leafPage.getCells());
        allCells.add(cell);
        allCells.sort(Comparator.comparing(c -> (Comparable) c.cellPayload().getKey()));

        int midIndex = allCells.size() / 2;
        leafPage.setCells(allCells.subList(0, midIndex));
        newLeafPage.setCells(allCells.subList(midIndex + 1, allCells.size()));

        Cell parentCell = IndexCell.createParentCell(leafPage.getPageNumber(), allCells.get(midIndex));
        handleParentPromotion(leafPage, newLeafPage, parentCell);

        leafPage.setSiblingPage((short) newPageId);

        writePage(leafPage, leafPage.getPageNumber());
        writePage(newLeafPage, newPageId);
    }

    private void handleInteriorOverflow(Page interiorPage, Cell cell) throws IOException {
        int newPageId = interiorPage.getPageNumber() + 1;
        Page newInteriorPage = new Page(PAGE_SIZE, (short) newPageId, PageTypes.INTERIOR_INDEX,
                interiorPage.getRootPage(), interiorPage.getParentPage(), interiorPage.getSiblingPage());

        List<Cell> allCells = new ArrayList<>(interiorPage.getCells());
        allCells.add(cell);
        allCells.sort(Comparator.comparing(c -> (Comparable) c.cellPayload().getKey()));

        int midIndex = allCells.size() / 2;
        interiorPage.setCells(allCells.subList(0, midIndex));
        newInteriorPage.setCells(allCells.subList(midIndex + 1, allCells.size()));

        Cell parentCell = IndexCell.createParentCell(interiorPage.getPageNumber(), allCells.get(midIndex));
        handleParentPromotion(interiorPage, newInteriorPage, parentCell);

        interiorPage.setSiblingPage((short) newPageId);

        writePage(interiorPage, interiorPage.getPageNumber());
        writePage(newInteriorPage, newPageId);
    }

    private void handleParentPromotion(Page leftPage, Page rightPage, Cell cell) throws IOException {
        if (leftPage.getParentPage() == (short) 0xFFFF) {
            int newRootPageId = rightPage.getPageNumber() + 1;
            Page newRootPage = new Page(PAGE_SIZE, (short) newRootPageId, PageTypes.INTERIOR_INDEX,
                    (short) newRootPageId, (short) 0xFFFF, rightPage.getPageNumber());

            newRootPage.insert(List.of(cell));
            leftPage.setParentPage((short) newRootPageId);
            rightPage.setParentPage((short) newRootPageId);
            writePage(newRootPage, newRootPageId);
        } else {
            Page parentPage = loadPage(leftPage.getParentPage());
            try {
                parentPage.insert(List.of(cell));
                parentPage.setSiblingPage(rightPage.getPageNumber());
                writePage(parentPage, parentPage.getPageNumber());
            } catch (DavisBaseException e) {
                handleOverflow(parentPage, cell);
            }
        }
    }

    public boolean isInitialized() throws IOException {
        return indexFile.length() > 0;
    }
}
