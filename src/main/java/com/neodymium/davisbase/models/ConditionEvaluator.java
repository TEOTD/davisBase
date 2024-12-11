package com.neodymium.davisbase.models;

import com.neodymium.davisbase.models.table.Row;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;

import java.util.Map;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class ConditionEvaluator {
    public static boolean evaluateRow(Row row, Map<String, String> conditions) {
        for (Map.Entry<String, String> entry : conditions.entrySet()) {
            String columnName = entry.getKey();
            String[] conditionParts = entry.getValue().split(" ", 2);
            String operator = conditionParts[0];
            String value = conditionParts[1];

            Object recordValue = row.data().get(columnName);

            if (!evaluateCondition(recordValue, operator, value)) {
                return false;
            }
        }
        return true;
    }

    private static boolean evaluateCondition(Object recordValue, String operator, String value) {
        if (recordValue instanceof Integer integerValue) {
            int intValue = Integer.parseInt(value);
            return switch (operator) {
                case "=" -> integerValue.equals(intValue);
                case "!=" -> !integerValue.equals(intValue);
                case "<" -> integerValue < intValue;
                case ">" -> integerValue > intValue;
                case "<=" -> integerValue <= intValue;
                case ">=" -> integerValue >= intValue;
                default -> false;
            };
        } else if (recordValue instanceof String stringValue) {
            return switch (operator) {
                case "=" -> stringValue.equals(value);
                case "!=" -> !stringValue.equals(value);
                case "LIKE" -> stringValue.contains(value.replace("%", ""));
                default -> false;
            };
        }
        return false;
    }
}
