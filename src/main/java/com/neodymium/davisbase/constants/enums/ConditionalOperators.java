package com.neodymium.davisbase.constants.enums;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.BiFunction;

public enum ConditionalOperators {
    EQUALS("=", (a, b) -> a != null && a.equals(b)),
    NOT_EQUALS("!=", (a, b) -> a != null && !a.equals(b)),
    LESS_THAN("<", (a, b) -> compareValues(a, b) < 0),
    GREATER_THAN(">", (a, b) -> compareValues(a, b) > 0),
    LESS_THAN_OR_EQUALS("<=", (a, b) -> compareValues(a, b) <= 0),
    GREATER_THAN_OR_EQUALS(">=", (a, b) -> compareValues(a, b) >= 0),
    LIKE("LIKE", (a, b) -> {
        if (a instanceof String strA && b instanceof String strB) {
            strB = strB.replace("%", "");
            return strA.contains(strB);
        }
        return false;
    }),
    IS("IS", (a, b) -> a == null && b == null || a != null && a.equals(b)),
    IS_NOT("IS NOT", (a, b) -> !(a == null && b == null) && (a == null || !a.equals(b)));

    private final String symbol;
    private final BiFunction<Object, Object, Boolean> operation;

    ConditionalOperators(String symbol, BiFunction<Object, Object, Boolean> operation) {
        this.symbol = symbol;
        this.operation = operation;
    }

    public static Optional<ConditionalOperators> fromSymbol(String symbol) {
        return Arrays.stream(ConditionalOperators.values())
                .filter(op -> op.getSymbol().equals(symbol))
                .findFirst();
    }

    private static int compareValues(Object a, Object b) {
        if (a == null || b == null) {
            throw new IllegalArgumentException("Cannot compare null values");
        }
        if (a instanceof Comparable && b instanceof Comparable) {
            Comparable<Object> compA = (Comparable<Object>) a;
            return compA.compareTo(b);
        }
        throw new IllegalArgumentException("Values are not comparable: " + a + " and " + b);
    }

    public String getSymbol() {
        return symbol;
    }

    public boolean apply(Object value1, Object value2) {
        return operation.apply(value1, value2);
    }
}