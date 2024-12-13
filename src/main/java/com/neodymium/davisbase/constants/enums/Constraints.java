package com.neodymium.davisbase.constants.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Arrays;

@Getter
@AllArgsConstructor
public enum Constraints {
    PRIMARY_KEY("PRIMARY KEY"),
    NOT_NULL("NOT NULL"),
    UNIQUE("UNIQUE");

    private final String value;

    public static Constraints getFromValue(String value) {
        return Arrays.stream(Constraints.values())
                .filter(c -> c.getValue().equals(value))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Invalid constraint: " + value));
    }
}