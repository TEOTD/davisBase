package com.neodymium.davisbase.models;

import com.neodymium.davisbase.error.DavisBaseException;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class Page<T extends TableRecord> {
    private final int pageSize;
    private final List<Short> cellOffsets;
    private final List<T> tableRecords;
    private final byte pageType;
    private final short rootPage;
    private final short parentPage;
    private final short siblingPage;
    private short numberOfCells;
    private short contentAreaStartCell;

    public Page(int pageSize, byte pageType, short rootPage, short parentPage, short siblingPage) {
        this.pageSize = pageSize;
        this.pageType = pageType;
        this.numberOfCells = 0;
        this.contentAreaStartCell = (short) pageSize;
        this.rootPage = rootPage;
        this.parentPage = parentPage;
        this.siblingPage = siblingPage;
        this.cellOffsets = new ArrayList<>();
        this.tableRecords = new ArrayList<>();
    }

    public static <T extends TableRecord> Page<T> create(int pageSize, byte pageType, short rootPage, short parentPage, short siblingPage) {
        return new Page<>(pageSize, pageType, rootPage, parentPage, siblingPage);
    }

    public static <T extends TableRecord> Page<T> deserialize(byte[] data, TableRecord<T> factory) {
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

            Page<T> page = new Page<>(
                    data.length,
                    pageType,
                    rootPage,
                    parentPage,
                    siblingPage
            );
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
        for (T tableRecord : tableRecords) {
            byte[] serializedRecord = tableRecord.serialize();
            contentAreaStartCell -= (short) serializedRecord.length;

            if (contentAreaStartCell < (16 + (2 * (numberOfCells + 1)))) {
                throw new DavisBaseException("Page full - not enough space for all records.");
            }

            this.tableRecords.add(tableRecord);
            cellOffsets.add(contentAreaStartCell);
        }
        numberOfCells += (short) tableRecords.size();
    }

    public void update(List<T> updatedRecords) {
        Map<String, T> updates = updatedRecords.stream()
                .collect(Collectors.toMap(T::getPrimaryKey, tableRecord -> tableRecord));
        List<T> newTableRecords = new ArrayList<>();
        short newContentAreaStart = (short) pageSize;
        List<Short> newCellOffsets = new ArrayList<>();

        for (T existingRecord : tableRecords) {
            T recordToInsert = updates.getOrDefault(existingRecord.getPrimaryKey(), existingRecord);

            byte[] serializedRecord = recordToInsert.serialize();
            newContentAreaStart -= (short) serializedRecord.length;

            if (newContentAreaStart < (16 + 2 * (newCellOffsets.size() + 1))) {
                throw new DavisBaseException("Page full - not enough space for updated records.");
            }

            newTableRecords.add(recordToInsert);
            newCellOffsets.add(newContentAreaStart);
        }

        tableRecords.clear();
        tableRecords.addAll(newTableRecords);
        cellOffsets.clear();
        cellOffsets.addAll(newCellOffsets);

        numberOfCells = (short) tableRecords.size();
        contentAreaStartCell = newContentAreaStart;
    }

    public void delete(List<String> primaryKeys) {
        List<T> toRemove = new ArrayList<>();
        for (String primaryKey : primaryKeys) {
            tableRecords.stream()
                    .filter(tableRecord -> tableRecord.getPrimaryKey().equals(primaryKey))
                    .findFirst()
                    .ifPresent(toRemove::add);
        }

        tableRecords.removeAll(toRemove);
        numberOfCells = (short) tableRecords.size();
        recalculateContentAreaStart();
    }

    public void truncate() {
        tableRecords.clear();
        cellOffsets.clear();
        numberOfCells = 0;
        contentAreaStartCell = (short) pageSize;
    }

    private byte[] serialize() {
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

    private void recalculateContentAreaStart() {
        contentAreaStartCell = (short) pageSize;
        for (T tableRecord : tableRecords) {
            contentAreaStartCell -= (short) tableRecord.serialize().length;
        }
    }

    private int offsetFor(T tableRecord) {
        return cellOffsets.get(tableRecords.indexOf(tableRecord));
    }
}

