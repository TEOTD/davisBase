package com.neodymium.davisbase.models.table;

import com.neodymium.davisbase.constants.enums.Constraints;

import java.util.Set;

public record Column(String name, byte typeCode, Set<Constraints> constraints) {

    public boolean isNullable() {
        return !constraints.contains(Constraints.NOT_NULL);
    }

    public boolean isPrimaryKey() {
        return constraints.contains(Constraints.PRIMARY_KEY);
    }

    public boolean isUnique() {
        return constraints.contains(Constraints.UNIQUE);
    }
}
