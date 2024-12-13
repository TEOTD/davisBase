package com.neodymium.davisbase.models.table;

import com.neodymium.davisbase.constants.enums.Constraints;
import com.neodymium.davisbase.constants.enums.DataTypes;

/**
 * Represents a single column in a table schema.
 */
public record Column(String name, DataTypes dataType, Constraints constraint) {

}
