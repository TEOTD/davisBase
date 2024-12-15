package com.neodymium.davisbase.models;

import com.neodymium.davisbase.constants.enums.ConditionalOperators;
import com.neodymium.davisbase.constants.enums.LogicalOperators;
import com.neodymium.davisbase.models.table.Clause;
import com.neodymium.davisbase.models.table.Condition;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class ConditionParser {
//    public static Map<String, String> parseCondition(String condition) {
//        Map<String, String> conditionMap = new HashMap<>();
//        String[] conditions = condition.split("\\s+AND\\s+|\\s+OR\\s+");
//        for (String cond : conditions) {
//            String[] parts = cond.trim().split("\\s+");
//            if (parts.length == 3) {
//                conditionMap.put(parts[0], parts[1] + " " + parts[2]);
//            }
//        }
//        return conditionMap;
//    }

    public static Clause parseCondition(String condition) {
        List<Condition> conditions = new ArrayList<>();
        List<LogicalOperators> conditionKeywords = new ArrayList<>();
        String[] tokens = condition.trim().split("\\s+");
        int i = 0;
        while (i < tokens.length) {
            String keyword = tokens[i].toUpperCase();
            Optional<ConditionalOperators> operation = ConditionalOperators.fromSymbol(keyword);
            if (keyword.equals(LogicalOperators.AND.getKeyword())) {
                conditionKeywords.add(LogicalOperators.AND);
                i++;
            } else if (keyword.equals(LogicalOperators.OR.getKeyword())) {
                conditionKeywords.add(LogicalOperators.OR);
                i++;
            } else if (operation.isPresent()) {
                ConditionalOperators op = operation.get();
                if (i == 0 || i >= tokens.length - 1) {
                    throw new IllegalArgumentException("Invalid condition format near operation: " + keyword);
                }
                String left = tokens[i - 1];
                String right = tokens[i + 1];

                if (ConditionalOperators.IS.equals(op)) {
                    if (ConditionalOperators.IS_NOT.getSymbol().equals(tokens[i] + " " + tokens[i + 1])) {
                        conditions.add(new Condition(ConditionalOperators.IS_NOT, left, null));
                        i += 3;
                    }
                    conditions.add(new Condition(ConditionalOperators.IS, left, null));
                    i += 2;
                } else {
                    conditions.add(new Condition(op, left, right));
                    i += 2;
                }
            } else {
                i++;
            }
        }

        return new Clause(conditionKeywords, conditions);
    }

}
