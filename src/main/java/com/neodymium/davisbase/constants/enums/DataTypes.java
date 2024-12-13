package com.neodymium.davisbase.constants.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Arrays;

@Getter
@AllArgsConstructor
public enum DataTypes {
    NULL(0, (byte) 0, "NULL", (byte) 0x00),
    TINYINT(1, (byte) 1, "TINYINT", (byte) 0x01),
    SMALLINT(2, (byte) 2, "SMALLINT", (byte) 0x02),
    INT(4, (byte) 4, "INT", (byte) 0x03),
    BIGINT(8, (byte) 8, "BIGINT", (byte) 0x04),
    LONG(8, (byte) 8, "LONG", (byte) 0x04),
    FLOAT(4, (byte) 4, "FLOAT", (byte) 0x05),
    DOUBLE(8, (byte) 8, "DOUBLE", (byte) 0x06),
    YEAR(1, (byte) 1, "YEAR", (byte) 0x08),
    TIME(4, (byte) 4, "TIME", (byte) 0x09),
    DATETIME(8, (byte) 8, "DATETIME", (byte) 0x0A),
    DATE(8, (byte) 8, "DATE", (byte) 0x0B),
    TEXT(0, (byte) 0, "TEXT", (byte) 0x0C);

    private final int size;
    private final byte sizeInBytes;
    private final String typeName;
    private final byte typeCode;

    public static DataTypes getFromTypeCode(byte typeCode) {
        return Arrays.stream(values())
                .filter(type -> type.typeCode == typeCode)
                .findFirst()
                .orElse(DataTypes.TEXT);
    }

    public static DataTypes getFromName(String name) {
        return Arrays.stream(values())
                .filter(type -> type.typeName.equalsIgnoreCase(name))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Invalid data type name: " + name));
    }
}