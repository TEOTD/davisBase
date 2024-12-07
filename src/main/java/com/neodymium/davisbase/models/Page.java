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
    private final List<Cell> tableRecords = new ArrayList<>();
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

    public static Page deserialize(byte[] data, int pageNumber, TableCellFactory factory) {
        try {
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

            List<Cell> tableRecords = new ArrayList<>(numberOfCells);
            for (short offset : cellOffsets) {
                buffer.position(offset);
                Cell tableRecord = factory.deserialize(buffer.array());
                tableRecords.add(tableRecord);
            }

            Page page = new Page(data.length, pageNumber, pageType, rootPage, parentPage, siblingPage);
            page.numberOfCells = numberOfCells;
            page.contentAreaStartCell = contentAreaStartCell;
            page.cellOffsets.addAll(cellOffsets);
            page.tableRecords.addAll(tableRecords);
            return page;
        } catch (Exception e) {
            log.error("Error during deserialization", e);
            throw new DavisBaseException("Failed to deserialize Page: " + e.getMessage());
        }
    }

    public void insert(List<Cell> tableRecords) {
        if (!hasEnoughSpaceForRecords(tableRecords)) {
            throw new DavisBaseException("Page full - not enough space for all records.");
        }

        List<RecordOffset> recordOffsets = new ArrayList<>();
        for (Cell tableRecord : tableRecords) {
            byte[] serializedRecord = tableRecord.serialize();
            contentAreaStartCell -= (short) serializedRecord.length;

            this.tableRecords.add(tableRecord);
            recordOffsets.add(new RecordOffset(contentAreaStartCell, tableRecord.primaryKey()));
        }
        recordOffsets.sort(Comparator.comparing(RecordOffset::primaryKey));
        cellOffsets.clear();
        for (RecordOffset recordOffset : recordOffsets) {
            cellOffsets.add((short) recordOffset.offset());
        }
        numberOfCells += (short) tableRecords.size();
    }

    public void update(List<Cell> updatedRecords) {
        int filledSpace = tableRecords.size() + cellOffsets.size() + 16;
        if (!hasEnoughSpaceForUpdatedRecords(filledSpace, updatedRecords, tableRecords)) {
            throw new DavisBaseException("Page full - not enough space for updated records.");
        }

        List<Cell> newTableRecords = new ArrayList<>();
        List<RecordOffset> recordOffsets = new ArrayList<>();
        short newContentAreaStart = (short) pageSize;

        for (Cell existingRecord : tableRecords) {
            Cell recordToInsert = updatedRecords.stream()
                    .filter(tableRecord -> tableRecord.primaryKey().equals(existingRecord.primaryKey()))
                    .findFirst()
                    .orElse(existingRecord);

            byte[] serializedRecord = recordToInsert.serialize();
            newContentAreaStart -= (short) serializedRecord.length;

            newTableRecords.add(recordToInsert);
            recordOffsets.add(new RecordOffset(newContentAreaStart, recordToInsert.primaryKey()));
        }

        recordOffsets.sort(Comparator.comparing(RecordOffset::primaryKey));
        List<Short> newCellOffsets = recordOffsets.stream()
                .map(offset -> (short) offset.offset())
                .toList();

        tableRecords.clear();
        tableRecords.addAll(newTableRecords);
        cellOffsets.clear();
        cellOffsets.addAll(newCellOffsets);

        numberOfCells = (short) tableRecords.size();
        contentAreaStartCell = newContentAreaStart;
    }

    public void delete(List<Integer> rowIds) {
        for (Cell tableRecord : tableRecords) {
            if (rowIds.contains(tableRecord.rowId())) {
                tableRecord.delete();
            }
        }
    }

    public void truncate() {
        for (Cell tableRecord : tableRecords) {
            tableRecord.delete();
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
        for (Cell tableRecord : tableRecords) {
            buffer.position(offsetFor(tableRecord));
            buffer.put(tableRecord.serialize());
        }

        return buffer.array();
    }

    private int offsetFor(Cell tableRecord) {
        return cellOffsets.get(tableRecords.indexOf(tableRecord));
    }

    private boolean hasEnoughSpaceForRecords(List<Cell> tableRecords) {
        short requiredSpace = 0;
        for (Cell tableRecord : tableRecords) {
            requiredSpace += (short) tableRecord.serialize().length;
        }
        int availableSpace = contentAreaStartCell - requiredSpace;
        return availableSpace >= (16 + 2 * (numberOfCells + tableRecords.size()));
    }

    private boolean hasEnoughSpaceForUpdatedRecords(int currentStart, List<Cell> newRecords, List<Cell> existingRecords) {
        short requiredSpace = 0;
        for (Cell tableRecord : existingRecords) {
            Cell recordToInsert = newRecords.stream()
                    .filter(r -> r.primaryKey().equals(tableRecord.primaryKey()))
                    .findFirst()
                    .orElse(tableRecord);

            requiredSpace += (short) recordToInsert.serialize().length;
        }
        int availableSpace = currentStart - requiredSpace;
        return availableSpace >= (16 + 2 * (existingRecords.size() + newRecords.size()));
    }

    public Optional<Cell> getTableRecord(int rowId) {
        return tableRecords.stream()
                .filter(tableRecord -> tableRecord.rowId() == rowId)
                .findFirst();
    }

    public short findChildPage(int rowId) {
        List<Cell> sortedTableRecords = new ArrayList<>(tableRecords);
        sortedTableRecords.sort(Comparator.comparing(Cell::rowId).reversed());
        return sortedTableRecords.stream()
                .filter(tableRecord -> tableRecord.rowId() < rowId)
                .findFirst()
                .map(Cell::pageNumber)
                .orElse(siblingPage);
    }

    public int getMaxRowId() {
        return tableRecords.stream()
                .mapToInt(Cell::rowId)
                .max()
                .orElseThrow(() -> new IllegalStateException("Table records are empty"));
    }


    public List<Cell> getTableCellsInRange(int minRowId, int maxRowId) {
        return tableRecords.stream()
                .filter(cell -> cell.rowId() >= minRowId && cell.rowId() <= maxRowId)
                .toList();
    }


    private record RecordOffset(int offset, String primaryKey) {
    }
}