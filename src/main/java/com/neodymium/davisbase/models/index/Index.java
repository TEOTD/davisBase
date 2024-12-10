package com.neodymium.davisbase.models.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Index {
    private final String columnName;
    private final Map<Object, List<Integer>> indexMap; // Maps column values to row IDs

    public Index(String columnName) {
        this.columnName = columnName;
        this.indexMap = new HashMap<>();
    }

    public void insert(Object value, int rowId) {
        indexMap.computeIfAbsent(value, k -> new ArrayList<>()).add(rowId);
    }

    public void delete(Object value, int rowId) {
        List<Integer> rowIds = indexMap.get(value);
        if (rowIds != null) {
            rowIds.remove((Integer) rowId);
            if (rowIds.isEmpty()) {
                indexMap.remove(value);
            }
        }
    }

    public List<Integer> search(Object value) {
        return indexMap.getOrDefault(value, Collections.emptyList());
    }

    public String getColumnName() {
        return columnName;
    }
}
