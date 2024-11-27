package com.neodymium.davisbase.models;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public record Employee(int rowId, String ssn, String firstName, char middleInitial, String lastName, String birthDate,
                       String address, char sex, int salary, short departmentNumber,
                       byte deletionMarker) implements TableRecord {

    @Override
    public String getPrimaryKey() {
        return ssn.replace("-", "");
    }

    @Override
    public byte[] toByteArray(int recordSize) {
        ByteBuffer buffer = ByteBuffer.allocate(recordSize);
        buffer.putInt(rowId);
        buffer.put(padString(ssn.replace("-", ""), 9));
        buffer.put(padString(firstName, 20));
        buffer.put((byte) middleInitial);
        buffer.put(padString(lastName, 20));
        buffer.put(padString(birthDate, 10));
        buffer.put(padString(address, 40));
        buffer.put((byte) sex);
        buffer.putInt(salary);
        buffer.putShort(departmentNumber);
        buffer.put(deletionMarker);

        return buffer.array();
    }

    private byte[] padString(String value, int length) {
        byte[] padded = new byte[length];
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        int bytesToCopy = Math.min(bytes.length, length);
        System.arraycopy(bytes, 0, padded, 0, bytesToCopy);
        return padded;
    }

}
