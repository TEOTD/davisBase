package com.neodymium.davisbase.constants.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Arrays;

@Getter
@AllArgsConstructor
public enum LogicalOperators {
    AND("AND"),
    OR("OR");

    private final String keyword;

    public static LogicalOperators fromKeyword(String keyword) {
        return Arrays.stream(LogicalOperators.values())
                .filter(logicalOperators -> logicalOperators.keyword.equals(keyword))
                .findFirst().orElse(null);
    }
}
