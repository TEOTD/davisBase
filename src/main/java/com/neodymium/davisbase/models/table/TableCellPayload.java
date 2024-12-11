package com.neodymium.davisbase.models.table;

import com.neodymium.davisbase.constants.enums.DataTypes;
import com.neodymium.davisbase.models.CellPayload;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

@Data
@AllArgsConstructor
public class TableCellPayload implements CellPayload {
    private byte deletionFlag;
    private byte noOfColumns;
    private List<DataTypes> dataTypes;
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

        List<DataTypes> dataTypes = new ArrayList<>();
        for (int i = 0; i < noOfColumns; i++) {
            if (buffer.remaining() < 4) {
                throw new IllegalArgumentException("Invalid payloadBytes: Insufficient data for data type");
            }
            int dataTypeId = buffer.getInt();
            DataTypes dataType = DataTypes.getFromTypeCode(dataTypeId);
            dataTypes.add(dataType);
        }
        return new TableCellPayload(deletionFlag, noOfColumns, dataTypes, buffer.array());
    }

    @Override
    public byte[] serialize() {
        int size = getSize();
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.put(deletionFlag);
        buffer.put(noOfColumns);

        for (DataTypes dataType : dataTypes) {
            buffer.putInt(dataType.getTypeCode());
        }
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
    public int getSize() {
        return 2 + (4 * dataTypes.size()) + body.length;
    }
}