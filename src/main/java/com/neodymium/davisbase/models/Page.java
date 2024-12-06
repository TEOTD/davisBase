package com.neodymium.davisbase.models;

import com.neodymium.davisbase.error.DavisBaseException;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

@Slf4j
@Getter
@Setter
public class Page<T extends Record> {
    private final int pageSize;
    private final int pageNumber;
    private final List<Short> cellOffsets;
    private final List<T> tableRecords;
    private final byte pageType;
    private short rootPage;
    private short parentPage;
    private short siblingPage;
    private short numberOfCells;
    private short contentAreaStartCell;

    public Page(int pageSize, int pageNumber, byte pageType, short rootPage, short parentPage, short siblingPage) {
        this.pageSize = pageSize;
        this.pageNumber = pageNumber;
        this.pageType = pageType;
        this.numberOfCells = 0;
        this.contentAreaStartCell = (short) pageSize;
        this.rootPage = rootPage;
        this.parentPage = parentPage;
        this.siblingPage = siblingPage;
        this.cellOffsets = new ArrayList<>();
        this.tableRecords = new ArrayList<>();
    }

    public static <T extends TableRecord> Page<T> deserialize(byte[] data, int pageNumber, TableRecordFactory<T> factory) {
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

            List<T> tableRecords = new ArrayList<>(numberOfCells);
            for (short offset : cellOffsets) {
                buffer.position(offset);
                T tableRecord = factory.deserialize(buffer.array());
                tableRecords.add(tableRecord);
            }

            Page<T> page = new Page<>(data.length, pageNumber, pageType, rootPage, parentPage, siblingPage);
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

    public void insert(List<T> tableRecords) {
        if (!hasEnoughSpaceForRecords(tableRecords)) {
            throw new DavisBaseException("Page full - not enough space for all records.");
        }

        List<RecordOffset> recordOffsets = new ArrayList<>();
        for (T tableRecord : tableRecords) {
            byte[] serializedRecord = tableRecord.serialize();
            contentAreaStartCell -= (short) serializedRecord.length;

            this.tableRecords.add(tableRecord);
            recordOffsets.add(new RecordOffset(contentAreaStartCell, tableRecord.getPrimaryKey()));
        }
        recordOffsets.sort(Comparator.comparing(RecordOffset::primaryKey));
        cellOffsets.clear();
        for (RecordOffset recordOffset : recordOffsets) {
            cellOffsets.add((short) recordOffset.offset());
        }
        numberOfCells += (short) tableRecords.size();
    }

    public void update(List<T> updatedRecords) {
        int filledSpace = tableRecords.size() + cellOffsets.size() + 16;
        if (!hasEnoughSpaceForUpdatedRecords(filledSpace, updatedRecords, tableRecords)) {
            throw new DavisBaseException("Page full - not enough space for updated records.");
        }

        List<T> newTableRecords = new ArrayList<>();
        List<RecordOffset> recordOffsets = new ArrayList<>();
        short newContentAreaStart = (short) pageSize;

        for (T existingRecord : tableRecords) {
            T recordToInsert = updatedRecords.stream()
                    .filter(tableRecord -> tableRecord.getPrimaryKey().equals(existingRecord.getPrimaryKey()))
                    .findFirst()
                    .orElse(existingRecord);

            byte[] serializedRecord = recordToInsert.serialize();
            newContentAreaStart -= (short) serializedRecord.length;

            newTableRecords.add(recordToInsert);
            recordOffsets.add(new RecordOffset(newContentAreaStart, recordToInsert.getPrimaryKey()));
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
        for (T tableRecord : tableRecords) {
            if (rowIds.contains(tableRecord.getRowId())) {
                tableRecord.delete();
            }
        }
    }

    public void truncate() {
        for (T tableRecord : tableRecords) {
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
        for (T tableRecord : tableRecords) {
            buffer.position(offsetFor(tableRecord));
            buffer.put(tableRecord.serialize());
        }

        return buffer.array();
    }

    private int offsetFor(T tableRecord) {
        return cellOffsets.get(tableRecords.indexOf(tableRecord));
    }

    private boolean hasEnoughSpaceForRecords(List<T> tableRecords) {
        short requiredSpace = 0;
        for (T tableRecord : tableRecords) {
            requiredSpace += (short) tableRecord.serialize().length;
        }
        int availableSpace = contentAreaStartCell - requiredSpace;
        return availableSpace >= (16 + 2 * (numberOfCells + tableRecords.size()));
    }

    private boolean hasEnoughSpaceForUpdatedRecords(int currentStart, List<T> newRecords, List<T> existingRecords) {
        short requiredSpace = 0;
        for (T tableRecord : existingRecords) {
            T recordToInsert = newRecords.stream()
                    .filter(r -> r.getPrimaryKey().equals(tableRecord.getPrimaryKey()))
                    .findFirst()
                    .orElse(tableRecord);

            requiredSpace += (short) recordToInsert.serialize().length;
        }
        int availableSpace = currentStart - requiredSpace;
        return availableSpace >= (16 + 2 * (existingRecords.size() + newRecords.size()));
    }

    public Optional<T> getTableRecord(int rowId) {
        return tableRecords.stream()
                .filter(tableRecord -> tableRecord.getRowId() == rowId)
                .findFirst();
    }

    public short findChildPage(int rowId) {
        return tableRecords.stream()
                .filter(tableRecord -> tableRecord.getRowId() < rowId)
                .findFirst();
    }

    private record RecordOffset(int offset, String primaryKey) {
    }
}