package com.neodymium.davisbase.models;

import com.neodymium.davisbase.error.DavisBaseException;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

@Slf4j
public record Page<T extends TableRecord>(List<T> tableRecords) {
    public byte[] toByteArray(int pageSize, int recordSize) {
        ByteBuffer buffer = ByteBuffer.allocate(pageSize);
        buffer.putShort((short) tableRecords.size());

        int recordEndOffset = pageSize;
        RecordOffset[] recordOffsets = new RecordOffset[tableRecords.size()];

        for (int i = 0; i < tableRecords.size(); i++) {
            T tableRecord = tableRecords.get(i);
            recordEndOffset -= recordSize;

            if (recordEndOffset < 0) {
                log.error("Page full during record addition");
                throw new DavisBaseException("Page full");
            }

            buffer.position(recordEndOffset);
            buffer.put(tableRecord.toByteArray(recordSize));
            recordOffsets[i] = new RecordOffset(recordEndOffset, tableRecord.getPrimaryKey());
        }

        Arrays.sort(recordOffsets, Comparator.comparing(RecordOffset::primaryKey));
        buffer.position(2);
        for (RecordOffset recordOffset : recordOffsets) {
            buffer.putShort((short) recordOffset.offset);
        }

        return buffer.array();
    }

    private record RecordOffset(int offset, String primaryKey) {
    }
}
