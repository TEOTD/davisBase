package com.neodymium.davisbase.models.index;

import com.neodymium.davisbase.models.CellPayload;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.util.ObjectUtils;

import java.nio.ByteBuffer;

@Data
@AllArgsConstructor
public class IndexCellPayload implements CellPayload {
    private Object key;
    private int rowId;

    public static CellPayload deserialize(byte[] payloadBytes) {
        if (payloadBytes == null || payloadBytes.length < 4) {
            throw new IllegalArgumentException("Invalid payloadBytes: Minimum 2 bytes required for metadata");
        }
        ByteBuffer buffer = ByteBuffer.wrap(payloadBytes);
        int rowId = buffer.getInt(payloadBytes.length - 4);
        int keyLength = payloadBytes.length - 4;
        Object key;

        switch (keyLength) {
            case 0 -> key = null;
            case 1 -> key = buffer.get();
            case 2 -> key = buffer.getShort();
            case 4 -> {
                byte[] rawBytes = new byte[4];
                buffer.get(rawBytes);
                int intValue = ByteBuffer.wrap(rawBytes).getInt();
                float floatValue = ByteBuffer.wrap(rawBytes).getFloat();
                key = (floatValue == (int) floatValue) ? intValue : floatValue;
            }
            case 8 -> {
                byte[] rawBytes = new byte[8];
                buffer.get(rawBytes);
                long longValue = ByteBuffer.wrap(rawBytes).getLong();
                double doubleValue = ByteBuffer.wrap(rawBytes).getDouble();
                key = (doubleValue == (long) doubleValue) ? longValue : doubleValue;
            }
            default -> {
                byte[] textBytes = new byte[keyLength];
                buffer.get(textBytes);
                key = new String(textBytes);
            }
        }

        return new IndexCellPayload(key, rowId);
    }

    @Override
    public byte[] serialize() {
        int size = getSize();
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.put((byte[]) key);
        buffer.putInt(rowId);
        return buffer.array();
    }

    @Override
    public void delete() {
    }

    @Override
    public boolean exists() {
        return true;
    }

    public int getSize() {
        return ObjectUtils.isEmpty(key) ? 4 : ((byte[]) key).length;
    }
}
