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
    TEXT(0, (byte) 0, "TEXT", 0x0C),
    TEXT1(1, (byte) 1, "TEXT1", 0x0D),
    TEXT2(2, (byte) 2, "TEXT2", 0x0E),
    TEXT3(3, (byte) 3, "TEXT3", 0x0F),
    TEXT4(4, (byte) 4, "TEXT4", 0x10),
    TEXT5(5, (byte) 5, "TEXT5", 0x11),
    TEXT6(6, (byte) 6, "TEXT6", 0x12),
    TEXT7(7, (byte) 7, "TEXT7", 0x13),
    TEXT8(8, (byte) 8, "TEXT8", 0x14),
    TEXT9(9, (byte) 9, "TEXT9", 0x15),
    TEXT10(10, (byte) 10, "TEXT10", 0x16),
    TEXT11(11, (byte) 11, "TEXT11", 0x17),
    TEXT12(12, (byte) 12, "TEXT12", 0x18),
    TEXT13(13, (byte) 13, "TEXT13", 0x19),
    TEXT14(14, (byte) 14, "TEXT14", 0x1A),
    TEXT15(15, (byte) 15, "TEXT15", 0x1B),
    TEXT16(16, (byte) 16, "TEXT16", 0x1C),
    TEXT17(17, (byte) 17, "TEXT17", 0x1D),
    TEXT18(18, (byte) 18, "TEXT18", 0x1E),
    TEXT19(19, (byte) 19, "TEXT19", 0x1F),
    TEXT20(20, (byte) 20, "TEXT20", 0x20),
    TEXT21(21, (byte) 21, "TEXT21", 0x21),
    TEXT22(22, (byte) 22, "TEXT22", 0x22),
    TEXT23(23, (byte) 23, "TEXT23", 0x23),
    TEXT24(24, (byte) 24, "TEXT24", 0x24),
    TEXT25(25, (byte) 25, "TEXT25", 0x25),
    TEXT26(26, (byte) 26, "TEXT26", 0x26),
    TEXT27(27, (byte) 27, "TEXT27", 0x27),
    TEXT28(28, (byte) 28, "TEXT28", 0x28),
    TEXT29(29, (byte) 29, "TEXT29", 0x29),
    TEXT30(30, (byte) 30, "TEXT30", 0x2A),
    TEXT31(31, (byte) 31, "TEXT31", 0x2B),
    TEXT32(32, (byte) 32, "TEXT32", 0x2C),
    TEXT33(33, (byte) 33, "TEXT33", 0x2D),
    TEXT34(34, (byte) 34, "TEXT34", 0x2E),
    TEXT35(35, (byte) 35, "TEXT35", 0x2F),
    TEXT36(36, (byte) 36, "TEXT36", 0x30),
    TEXT37(37, (byte) 37, "TEXT37", 0x31),
    TEXT38(38, (byte) 38, "TEXT38", 0x32),
    TEXT39(39, (byte) 39, "TEXT39", 0x33),
    TEXT40(40, (byte) 40, "TEXT40", 0x34),
    TEXT41(41, (byte) 41, "TEXT41", 0x35),
    TEXT42(42, (byte) 42, "TEXT42", 0x36),
    TEXT43(43, (byte) 43, "TEXT43", 0x37),
    TEXT44(44, (byte) 44, "TEXT44", 0x38),
    TEXT45(45, (byte) 45, "TEXT45", 0x39),
    TEXT46(46, (byte) 46, "TEXT46", 0x3A),
    TEXT47(47, (byte) 47, "TEXT47", 0x3B),
    TEXT48(48, (byte) 48, "TEXT48", 0x3C),
    TEXT49(49, (byte) 49, "TEXT49", 0x3D),
    TEXT50(50, (byte) 50, "TEXT50", 0x3E),
    TEXT51(51, (byte) 51, "TEXT51", 0x3F),
    TEXT52(52, (byte) 52, "TEXT52", 0x40),
    TEXT53(53, (byte) 53, "TEXT53", 0x41),
    TEXT54(54, (byte) 54, "TEXT54", 0x42),
    TEXT55(55, (byte) 55, "TEXT55", 0x43),
    TEXT56(56, (byte) 56, "TEXT56", 0x44),
    TEXT57(57, (byte) 57, "TEXT57", 0x45),
    TEXT58(58, (byte) 58, "TEXT58", 0x46),
    TEXT59(59, (byte) 59, "TEXT59", 0x47),
    TEXT60(60, (byte) 60, "TEXT60", 0x48),
    TEXT61(61, (byte) 61, "TEXT61", 0x49),
    TEXT62(62, (byte) 62, "TEXT62", 0x4A),
    TEXT63(63, (byte) 63, "TEXT63", 0x4B),
    TEXT64(64, (byte) 64, "TEXT64", 0x4C),
    TEXT65(65, (byte) 65, "TEXT65", 0x4D),
    TEXT66(66, (byte) 66, "TEXT66", 0x4E),
    TEXT67(67, (byte) 67, "TEXT67", 0x4F),
    TEXT68(68, (byte) 68, "TEXT68", 0x50),
    TEXT69(69, (byte) 69, "TEXT69", 0x51),
    TEXT70(70, (byte) 70, "TEXT70", 0x52),
    TEXT71(71, (byte) 71, "TEXT71", 0x53),
    TEXT72(72, (byte) 72, "TEXT72", 0x54),
    TEXT73(73, (byte) 73, "TEXT73", 0x55),
    TEXT74(74, (byte) 74, "TEXT74", 0x56),
    TEXT75(75, (byte) 75, "TEXT75", 0x57),
    TEXT76(76, (byte) 76, "TEXT76", 0x58),
    TEXT77(77, (byte) 77, "TEXT77", 0x59),
    TEXT78(78, (byte) 78, "TEXT78", 0x5A),
    TEXT79(79, (byte) 79, "TEXT79", 0x5B),
    TEXT80(80, (byte) 80, "TEXT80", 0x5C),
    TEXT81(81, (byte) 81, "TEXT81", 0x5D),
    TEXT82(82, (byte) 82, "TEXT82", 0x5E),
    TEXT83(83, (byte) 83, "TEXT83", 0x5F),
    TEXT84(84, (byte) 84, "TEXT84", 0x60),
    TEXT85(85, (byte) 85, "TEXT85", 0x61),
    TEXT86(86, (byte) 86, "TEXT86", 0x62),
    TEXT87(87, (byte) 87, "TEXT87", 0x63),
    TEXT88(88, (byte) 88, "TEXT88", 0x64),
    TEXT89(89, (byte) 89, "TEXT89", 0x65),
    TEXT90(90, (byte) 90, "TEXT90", 0x66),
    TEXT91(91, (byte) 91, "TEXT91", 0x67),
    TEXT92(92, (byte) 92, "TEXT92", 0x68),
    TEXT93(93, (byte) 93, "TEXT93", 0x69),
    TEXT94(94, (byte) 94, "TEXT94", 0x6A),
    TEXT95(95, (byte) 95, "TEXT95", 0x6B),
    TEXT96(96, (byte) 96, "TEXT96", 0x6C),
    TEXT97(97, (byte) 97, "TEXT97", 0x6D),
    TEXT98(98, (byte) 98, "TEXT98", 0x6E),
    TEXT99(99, (byte) 99, "TEXT99", 0x6F),
    TEXT100(100, (byte) 100, "TEXT100", 0x70),
    TEXT101(101, (byte) 101, "TEXT101", 0x71),
    TEXT102(102, (byte) 102, "TEXT102", 0x72),
    TEXT103(103, (byte) 103, "TEXT103", 0x73),
    TEXT104(104, (byte) 104, "TEXT104", 0x74),
    TEXT105(105, (byte) 105, "TEXT105", 0x75),
    TEXT106(106, (byte) 106, "TEXT106", 0x76),
    TEXT107(107, (byte) 107, "TEXT107", 0x77),
    TEXT108(108, (byte) 108, "TEXT108", 0x78),
    TEXT109(109, (byte) 109, "TEXT109", 0x79),
    TEXT110(110, (byte) 110, "TEXT110", 0x7A),
    TEXT111(111, (byte) 111, "TEXT111", 0x7B),
    TEXT112(112, (byte) 112, "TEXT112", 0x7C),
    TEXT113(113, (byte) 113, "TEXT113", 0x7D),
    TEXT114(114, (byte) 114, "TEXT114", 0x7E),
    TEXT115(115, (byte) 115, "TEXT115", 0x7F);

    private final int size;
    private final byte sizeInBytes;
    private final String typeName;
    private final int typeCode;

    public static DataTypes getFromTypeCode(int dataTypeId) {
        return Arrays.stream(DataTypes.values())
                .filter(type -> type.getTypeCode() == dataTypeId)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Invalid data type code: " + dataTypeId));

    }

    public static DataTypes getFromTextTypeSize(int textTypeSize) {
        return DataTypes.getFromTypeCode(0x0C + textTypeSize);
    }

    public static DataTypes getFromName(String name) {
        return Arrays.stream(DataTypes.values())
                .filter(type -> type.getTypeName().equalsIgnoreCase(name))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Invalid data type name: " + name));
    }
}