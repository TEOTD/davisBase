package com.neodymium.davisbase.models;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;

import java.util.HashMap;
import java.util.Map;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class ConditionParser {
    public static Map<String, String> parseCondition(String condition) {
        Map<String, String> conditionMap = new HashMap<>();
        String[] conditions = condition.split("\\s+AND\\s+|\\s+OR\\s+");
        for (String cond : conditions) {
            String[] parts = cond.trim().split("\\s+");
            if (parts.length == 3) {
                conditionMap.put(parts[0], parts[1] + " " + parts[2]);
            }
        }
        return conditionMap;
    }
}
