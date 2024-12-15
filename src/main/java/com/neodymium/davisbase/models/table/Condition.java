package com.neodymium.davisbase.models.table;

import com.neodymium.davisbase.constants.enums.ConditionalOperators;

public record Condition(ConditionalOperators operation, String leftHandSideValue, String rightHandSideValue) {
}