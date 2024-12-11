package com.neodymium.davisbase.constants.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Arrays;

@Getter
@AllArgsConstructor
public enum PageTypes {
    INTERIOR((byte) 5),
    INTERIOR_INDEX((byte) 2),
    LEAF((byte) 13),
    LEAF_INDEX((byte) 10);

    private final byte value;

    public static PageTypes get(byte value) {
        return Arrays.stream(PageTypes.values())
                .filter(v -> v.getValue() == value)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("received invalid byte for page type"));
    }
}
