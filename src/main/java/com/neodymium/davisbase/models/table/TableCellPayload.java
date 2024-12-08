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
    private List<byte[]> body;

    public static CellPayload deserialize(byte[] payloadBytes) {
        if (payloadBytes == null || payloadBytes.length < 5) {
            throw new IllegalArgumentException("Invalid payloadBytes: Not enough data to deserialize");
        }

        ByteBuffer buffer = ByteBuffer.wrap(payloadBytes);
        byte deletionFlag = buffer.get();
        byte noOfColumns = buffer.get();

        List<DataTypes> dataTypes = new ArrayList<>();
        List<Integer> dataSizes = new ArrayList<>();

        for (int i = 0; i < noOfColumns; i++) {
            if (buffer.remaining() < 4) {
                throw new IllegalArgumentException("Invalid payloadBytes: Insufficient data for data types");
            }
            int dataTypeId = buffer.getInt();
            DataTypes dataType = DataTypes.getFromTypeCode(dataTypeId);
            dataTypes.add(dataType);

            int dataSize = (DataTypes.TEXT.equals(dataType)) ? (dataTypeId - dataType.getTypeCode()) : dataType.getSize();
            dataSizes.add(dataSize);
        }

        List<byte[]> body = new ArrayList<>();

        for (int i = 0; i < noOfColumns; i++) {
            if (buffer.remaining() < dataSizes.get(i)) {
                throw new IllegalArgumentException("Invalid payloadBytes: Data size exceeds remaining bytes");
            }
            byte[] columnData = new byte[dataSizes.get(i)];
            buffer.get(columnData);
            body.add(columnData);
        }

        return new TableCellPayload(deletionFlag, noOfColumns, dataTypes, body);
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
        for (byte[] bodyData : body) {
            buffer.put(bodyData);
        }
        return buffer.array();
    }

    @Override
    public void delete() {
        if (deletionFlag == 0) {
            deletionFlag = 1;
        }
    }

    @Override
    public boolean exists() {
        return deletionFlag != 1;
    }

    @Override
    public int getSize() {
        return 2 + (4 * dataTypes.size()) + body.stream().mapToInt(b -> b.length).sum();
    }
}