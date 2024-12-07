package com.neodymium.davisbase.constants.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

@Getter
@AllArgsConstructor
public enum DataTypes {
    NULL((byte) 0) {
        @Override
        public String toString() {
            return "NULL";
        }
    },
    TINYINT((byte) 1) {
        @Override
        public String toString() {
            return "TINYINT";
        }
    },
    SMALLINT((byte) 2) {
        @Override
        public String toString() {
            return "SMALLINT";
        }
    },
    INT((byte) 3) {
        @Override
        public String toString() {
            return "INT";
        }
    },
    BIGINT((byte) 4) {
        @Override
        public String toString() {
            return "BIGINT";
        }
    },
    FLOAT((byte) 5) {
        @Override
        public String toString() {
            return "FLOAT";
        }
    },
    DOUBLE((byte) 6) {
        @Override
        public String toString() {
            return "DOUBLE";
        }
    },
    YEAR((byte) 8) {
        @Override
        public String toString() {
            return "YEAR";
        }
    },
    TIME((byte) 9) {
        @Override
        public String toString() {
            return "TIME";
        }
    },
    DATETIME((byte) 10) {
        @Override
        public String toString() {
            return "DATETIME";
        }
    },
    DATE((byte) 11) {
        @Override
        public String toString() {
            return "DATE";
        }
    },
    TEXT((byte) 12) {
        @Override
        public String toString() {
            return "TEXT";
        }
    };

    private static final Map<Byte, DataTypes> dataTypeLookup = new HashMap<>();
    private static final Map<Byte, Integer> dataTypeSizeLookup = new HashMap<>();
    private static final Map<String, DataTypes> dataTypeStringLookup = new HashMap<>();
    private static final Map<DataTypes, Integer> dataTypePrintOffset = new EnumMap<>(DataTypes.class);


    static {
        for (DataTypes s : DataTypes.values()) {
            dataTypeLookup.put(s.getValue(), s);
            dataTypeStringLookup.put(s.toString(), s);

            if (s == DataTypes.TINYINT || s == DataTypes.YEAR) {
                dataTypeSizeLookup.put(s.getValue(), 1);
                dataTypePrintOffset.put(s, 6);
            } else if (s == DataTypes.SMALLINT) {
                dataTypeSizeLookup.put(s.getValue(), 2);
                dataTypePrintOffset.put(s, 8);
            } else if (s == DataTypes.INT || s == DataTypes.FLOAT || s == DataTypes.TIME) {
                dataTypeSizeLookup.put(s.getValue(), 4);
                dataTypePrintOffset.put(s, 10);
            } else if (s == DataTypes.BIGINT || s == DataTypes.DOUBLE
                    || s == DataTypes.DATETIME || s == DataTypes.DATE) {
                dataTypeSizeLookup.put(s.getValue(), 8);
                dataTypePrintOffset.put(s, 25);
            } else if (s == DataTypes.TEXT) {
                dataTypePrintOffset.put(s, 25);
            } else if (s == DataTypes.NULL) {
                dataTypeSizeLookup.put(s.getValue(), 0);
                dataTypePrintOffset.put(s, 6);
            }
        }
    }

    private final byte value;

    public static DataTypes get(byte value) {
        if (value > 12)
            return DataTypes.TEXT;
        return dataTypeLookup.get(value);
    }

    public static DataTypes get(String text) {
        return dataTypeStringLookup.get(text);
    }

    public static int getLength(DataTypes type) {
        return getLength(type.getValue());
    }

    public static int getLength(byte value) {
        if (get(value) != DataTypes.TEXT)
            return dataTypeSizeLookup.get(value);
        else
            return value - 12;
    }

    public int getPrintOffset() {
        return dataTypePrintOffset.get(get(this.value));
    }

}
