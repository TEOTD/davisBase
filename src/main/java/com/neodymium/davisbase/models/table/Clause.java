package com.neodymium.davisbase.models.table;

import com.neodymium.davisbase.constants.enums.LogicalOperators;

import java.util.List;

public record Clause(List<LogicalOperators> conditionKeywords, List<Condition> conditions) {
}
