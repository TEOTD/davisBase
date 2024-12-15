package com.neodymium.davisbase.models.table;

import com.neodymium.davisbase.models.CellPayload;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.nio.ByteBuffer;

@Data
@AllArgsConstructor
public class TableCellPayload implements CellPayload {
    private byte deletionFlag;
    private byte noOfColumns;
    private byte[] typeCodes;
    private byte[] body;

    public static CellPayload deserialize(byte[] payloadBytes) {
        if (payloadBytes == null || payloadBytes.length < 2) {
            throw new IllegalArgumentException("Invalid payloadBytes: Minimum 2 bytes required for metadata");
        }
        ByteBuffer buffer = ByteBuffer.wrap(payloadBytes);
        byte deletionFlag = buffer.get();
        byte noOfColumns = buffer.get();

        if (noOfColumns < 0) {
            throw new IllegalArgumentException("Invalid payloadBytes: Negative column count");
        }

        byte[] typeCodes = new byte[noOfColumns];
        for (int i = 0; i < noOfColumns; i++) {
            typeCodes[i] = buffer.get();
        }
        byte[] body = new byte[buffer.remaining()];
        buffer.get(body);
        return new TableCellPayload(deletionFlag, noOfColumns, typeCodes, body);
    }

    @Override
    public byte[] serialize() {
        int size = getSize();
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.put(deletionFlag);
        buffer.put(noOfColumns);
        buffer.put(typeCodes);
        buffer.put(body);
        return buffer.array();
    }

    @Override
    public void delete() {
        deletionFlag = 1;
    }

    @Override
    public boolean exists() {
        return deletionFlag == 0;
    }

    @Override
    public int getRowId() {
        return 0;
    }

    @Override
    public Object getKey() {
        return null;
    }

    public int getSize() {
        return 2 + typeCodes.length + body.length;
    }
}