package com.neodymium.davisbase.models.table;

import com.neodymium.davisbase.constants.enums.Constraints;
import com.neodymium.davisbase.constants.enums.DataTypes;

import java.util.Set;

public record Column(String name, DataTypes dataType, Set<Constraints> constraints) {

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
