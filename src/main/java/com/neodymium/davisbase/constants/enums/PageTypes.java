package com.neodymium.davisbase.constants.enums;

import java.util.HashMap;
import java.util.Map;

public enum PageTypes {
    INTERIOR((byte) 5),
    INTERIOR_INDEX((byte) 2),
    LEAF((byte) 13),
    LEAF_INDEX((byte) 10);

    private static final Map<Byte, PageTypes> pageTypeLookup = new HashMap<>();

    static {
        for (PageTypes pageTypes : PageTypes.values())
            pageTypeLookup.put(pageTypes.getValue(), pageTypes);
    }

    private final byte value;

    PageTypes(byte value) {
        this.value = value;
    }

    public static PageTypes get(byte value) {
        return pageTypeLookup.get(value);
    }

    public byte getValue() {
        return value;
    }
}
