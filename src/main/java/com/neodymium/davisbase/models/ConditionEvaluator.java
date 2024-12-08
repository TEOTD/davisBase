package com.neodymium.davisbase.models;

import java.util.Map;

public class ConditionEvaluator {
    public static boolean evaluateRow(TableRecord record, Map<String, String> conditions) {
        for (Map.Entry<String, String> entry : conditions.entrySet()) {
            String columnName = entry.getKey();
            String[] conditionParts = entry.getValue().split(" ", 2);
            String operator = conditionParts[0];
            String value = conditionParts[1];

            Object recordValue = record.getValue(columnName);

            if (!evaluateCondition(recordValue, operator, value)) {
                return false;
            }
        }
        return true;
    }

    private static boolean evaluateCondition(Object recordValue, String operator, String value) {
        if (recordValue instanceof Integer) {
            int intValue = Integer.parseInt(value);
            return switch (operator) {
                case "=" -> ((Integer) recordValue).equals(intValue);
                case "!=" -> !((Integer) recordValue).equals(intValue);
                case "<" -> ((Integer) recordValue) < intValue;
                case ">" -> ((Integer) recordValue) > intValue;
                case "<=" -> ((Integer) recordValue) <= intValue;
                case ">=" -> ((Integer) recordValue) >= intValue;
                default -> false;
            };
        } else if (recordValue instanceof String) {
            return switch (operator) {
                case "=" -> recordValue.equals(value);
                case "!=" -> !recordValue.equals(value);
                case "LIKE" -> ((String) recordValue).contains(value.replace("%", ""));
                default -> false;
            };
        }
        return false;
    }
}


