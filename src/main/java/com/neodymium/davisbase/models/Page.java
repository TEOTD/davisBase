package com.neodymium.davisbase.models;

import com.neodymium.davisbase.error.DavisBaseException;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public record Page<T extends TableRecord>(
        int pageSize,
        byte pageType,
        short numberOfCells,
        short contentAreaStartCell,
        short rootPage,
        short parentPage,
        short siblingPage,
        List<Short> cellOffsets,
        List<T> tableRecords
) {
    static <T extends TableRecord> Page<T> deserialize(byte[] data, TableRecordFactory<T> factory) {
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
                T tableRecord = factory.createFromBytes(buffer);
                tableRecords.add(tableRecord);
            }

            return new Page<>(
                    data.length,
                    pageType,
                    numberOfCells,
                    contentAreaStartCell,
                    rootPage,
                    parentPage,
                    siblingPage,
                    cellOffsets,
                    tableRecords
            );
        } catch (Exception e) {
            log.error("Error during deserialization", e);
            throw new DavisBaseException("Failed to deserialize Page: " + e.getMessage());
        }
    }

    byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(pageSize);
        buffer.put(pageType);
        buffer.put((byte) 0x00);
        buffer.putShort((short) tableRecords.size());
        buffer.putShort((short) pageSize);
        buffer.putInt(0x00);
        buffer.putShort(rootPage);
        buffer.putShort(parentPage);
        buffer.putShort(siblingPage);

        int contentAreaPointer = pageSize;
        RecordOffset[] recordOffsets = new RecordOffset[tableRecords.size()];
        for (int i = 0; i < tableRecords.size(); i++) {
            T tableRecord = tableRecords.get(i);
            byte[] serializedRecord = tableRecord.serialize();
            contentAreaPointer -= serializedRecord.length;

            if (contentAreaPointer < buffer.position() + (2 * tableRecords.size())) {
                log.error("Page full during record addition");
                throw new DavisBaseException("Page full - not enough space for all records.");
            }

            buffer.position(contentAreaPointer);
            buffer.put(serializedRecord);
            recordOffsets[i] = new RecordOffset(contentAreaPointer, tableRecord.getPrimaryKey());
        }

        buffer.position(16);
        for (RecordOffset recordOffset : recordOffsets) {
            buffer.putShort((short) recordOffset.offset);
        }
        buffer.putShort(4, (short) contentAreaPointer);
        return buffer.array();
    }

    public interface TableRecordFactory<T> {
        T createFromBytes(ByteBuffer buffer);
    }

    private record RecordOffset(int offset, String primaryKey) {
    }
}
