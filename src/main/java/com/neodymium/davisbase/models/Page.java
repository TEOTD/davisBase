package com.neodymium.davisbase.models;

import com.neodymium.davisbase.error.DavisBaseException;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

@Data
@Slf4j
@RequiredArgsConstructor
public class Page {
    private final int pageSize;
    private final int pageNumber;
    private final byte pageType;
    private final List<Short> cellOffsets = new ArrayList<>();
    private final List<Cell> cells = new ArrayList<>();
    private short rootPage;
    private short parentPage;
    private short siblingPage;
    private short numberOfCells = 0;
    private short contentAreaStartCell = (short) pageSize;

    public Page(int pageSize, int pageNumber, byte pageType, short rootPage, short parentPage, short siblingPage) {
        this.pageSize = pageSize;
        this.pageNumber = pageNumber;
        this.pageType = pageType;
        this.rootPage = rootPage;
        this.parentPage = parentPage;
        this.siblingPage = siblingPage;
    }

    public static Page deserialize(byte[] data, int pageNumber, CellFactory factory) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        byte pageType = buffer.get();
        buffer.get();
        short numberOfCells = buffer.getShort();
        short contentAreaStartCell = buffer.getShort();
        buffer.getInt();
        short rootPage = buffer.getShort();
        short parentPage = buffer.getShort();
        short siblingPage = buffer.getShort();

        List<Short> cellOffsets = new ArrayList<>(numberOfCells);
        for (int i = 0; i < numberOfCells; i++) {
            cellOffsets.add(buffer.getShort());
        }

        List<Cell> cells = new ArrayList<>(numberOfCells);
        for (short offset : cellOffsets) {
            buffer.position(offset);
            Cell cell = factory.deserialize(buffer.array());
            cells.add(cell);
        }

        Page page = new Page(data.length, pageNumber, pageType, rootPage, parentPage, siblingPage);
        page.numberOfCells = numberOfCells;
        page.contentAreaStartCell = contentAreaStartCell;
        page.cellOffsets.addAll(cellOffsets);
        page.cells.addAll(cells);
        return page;
    }

    public void insert(List<Cell> cells) {
        if (!hasEnoughSpaceForCells(cells)) {
            throw new DavisBaseException("Page full - not enough space for all cells.");
        }

        List<CellOffset> cellOffsets = new ArrayList<>();
        for (Cell cell : cells) {
            byte[] serializedCell = cell.serialize();
            contentAreaStartCell -= (short) serializedCell.length;

            this.cells.add(cell);
            cellOffsets.add(new CellOffset(contentAreaStartCell, cell.primaryKey()));
        }
        cellOffsets.sort(Comparator.comparing(CellOffset::primaryKey));
        this.cellOffsets.clear();
        for (CellOffset cellOffset : cellOffsets) {
            this.cellOffsets.add((short) cellOffset.offset());
        }
        numberOfCells += (short) cells.size();
    }

    public void update(List<Cell> cells) {
        int filledSpace = this.cells.size() + this.cellOffsets.size() + 16;
        if (!hasEnoughSpaceForUpdatedCells(filledSpace, cells, this.cells)) {
            throw new DavisBaseException("Page full - not enough space for updated cells.");
        }

        List<Cell> newCells = new ArrayList<>();
        List<CellOffset> cellOffsets = new ArrayList<>();
        short newContentAreaStart = (short) pageSize;

        for (Cell existingCell : this.cells) {
            Cell cellToInsert = cells.stream()
                    .filter(cell -> cell.primaryKey().equals(existingCell.primaryKey()))
                    .findFirst()
                    .orElse(existingCell);

            byte[] serializedCell = cellToInsert.serialize();
            newContentAreaStart -= (short) serializedCell.length;

            newCells.add(cellToInsert);
            cellOffsets.add(new CellOffset(newContentAreaStart, cellToInsert.primaryKey()));
        }

        cellOffsets.sort(Comparator.comparing(CellOffset::primaryKey));
        List<Short> newCellOffsets = cellOffsets.stream()
                .map(offset -> (short) offset.offset())
                .toList();

        this.cells.clear();
        this.cells.addAll(newCells);
        this.cellOffsets.clear();
        this.cellOffsets.addAll(newCellOffsets);

        numberOfCells = (short) this.cells.size();
        contentAreaStartCell = newContentAreaStart;
    }

    public void delete(List<Integer> rowIds) {
        for (Cell cell : cells) {
            if (rowIds.contains(cell.rowId())) {
                cell.delete();
            }
        }
    }

    public void truncate() {
        for (Cell cell : cells) {
            cell.delete();
        }
    }

    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(pageSize);
        buffer.put(pageType);
        buffer.put((byte) 0x00);
        buffer.putShort(numberOfCells);
        buffer.putShort(contentAreaStartCell);
        buffer.putInt(0x00);
        buffer.putShort(rootPage);
        buffer.putShort(parentPage);
        buffer.putShort(siblingPage);
        for (short offset : cellOffsets) {
            buffer.putShort(offset);
        }
        for (Cell cell : cells) {
            buffer.position(offsetFor(cell));
            buffer.put(cell.serialize());
        }

        return buffer.array();
    }

    private int offsetFor(Cell cell) {
        return cellOffsets.get(cells.indexOf(cell));
    }

    private boolean hasEnoughSpaceForCells(List<Cell> cells) {
        short requiredSpace = 0;
        for (Cell cell : cells) {
            requiredSpace += (short) cell.serialize().length;
        }
        int availableSpace = contentAreaStartCell - requiredSpace;
        return availableSpace >= (16 + 2 * (numberOfCells + cells.size()));
    }

    private boolean hasEnoughSpaceForUpdatedCells(int currentStart, List<Cell> newCells, List<Cell> existingCells) {
        short requiredSpace = 0;
        for (Cell existingCell : existingCells) {
            Cell cellToInsert = newCells.stream()
                    .filter(r -> r.primaryKey().equals(existingCell.primaryKey()))
                    .findFirst()
                    .orElse(existingCell);

            requiredSpace += (short) cellToInsert.serialize().length;
        }
        int availableSpace = currentStart - requiredSpace;
        return availableSpace >= (16 + 2 * (existingCells.size() + newCells.size()));
    }

    public Optional<Cell> getTableCell(int rowId) {
        return cells.stream()
                .filter(cell -> cell.rowId() == rowId)
                .findFirst();
    }

    public short findChildPage(int rowId) {
        List<Cell> sortedCells = new ArrayList<>(cells);
        sortedCells.sort(Comparator.comparing(Cell::rowId).reversed());
        return sortedCells.stream()
                .filter(cell -> cell.rowId() < rowId)
                .findFirst()
                .map(Cell::pageNumber)
                .orElse(siblingPage);
    }

    public int getMaxRowId() {
        return cells.stream()
                .mapToInt(Cell::rowId)
                .max()
                .orElseThrow(() -> new IllegalStateException("Table cells are empty"));
    }


    public List<Cell> getTableCellsInRange(int minRowId, int maxRowId) {
        return cells.stream()
                .filter(cell -> cell.rowId() >= minRowId && cell.rowId() <= maxRowId)
                .toList();
    }


    private record CellOffset(int offset, String primaryKey) {
    }
}