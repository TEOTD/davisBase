package com.neodymium.davisbase.constants.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Arrays;

@Getter
@AllArgsConstructor
public enum DataTypes {
    NULL(0, (byte) 0, "NULL", 0x00),
    TINYINT(1, (byte) 1, "TINYINT", 0x01),
    SMALLINT(2, (byte) 2, "SMALLINT", 0x02),
    INT(4, (byte) 4, "INT", 0x03),
    BIGINT(8, (byte) 8, "BIGINT", 0x04),
    LONG(8, (byte) 8, "LONG", 0x04),
    FLOAT(4, (byte) 4, "FLOAT", 0x05),
    DOUBLE(8, (byte) 8, "DOUBLE", 0x06),
    YEAR(1, (byte) 1, "YEAR", 0x08),
    TIME(4, (byte) 4, "TIME", 0x09),
    DATETIME(8, (byte) 8, "DATETIME", 0x0A),
    DATE(8, (byte) 8, "DATE", 0x0B),
    TEXT(-1, (byte) -1, "TEXT", 0x0C);

    private final int size;
    private final byte sizeInBytes;
    private final String typeName;
    private final int typeCode;

    public static DataTypes getFromTypeCode(int dataTypeId) {
        if (dataTypeId < 0x0C) {
            return Arrays.stream(DataTypes.values())
                    .filter(type -> type.getTypeCode() == dataTypeId)
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Invalid data type code: " + dataTypeId));
        }
        return DataTypes.TEXT;
    }
}