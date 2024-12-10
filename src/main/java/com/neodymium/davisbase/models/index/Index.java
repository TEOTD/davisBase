package com.neodymium.davisbase.models.index;

import com.neodymium.davisbase.models.Cell;
import com.neodymium.davisbase.models.table.Column;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Data
@AllArgsConstructor
public class Index {
    private String tableName;
    private String columnName;
    private BTree btree;

    public Index(String tableName, String columnName, RandomAccessFile file) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.btree = new BTree(file);
    }

    public void create() throws IOException {
        if (btree.isInitialized()) {
            return;
        }
        btree.create();
//        Table table = new Table(tableName);
//        List<Map<Column, Object>> result = table.select(List.of("rowId", columnName), null);
        List<Map<Column, Object>> result = new ArrayList<>();
        List<Cell> cells = new ArrayList<>();
        for (Map<Column, Object> row : result) {
            Integer rowId = null;
            Object columnValue = null;

            for (Map.Entry<Column, Object> entry : row.entrySet()) {
                Column column = entry.getKey();
                Object value = entry.getValue();

                if (column.name().equals("rowId")) {
                    rowId = (Integer) value;
                } else if (column.name().equals(columnName)) {
                    columnValue = value;
                }
            }
            if (rowId != null && columnValue != null) {
                IndexCell cell = new IndexCell(
                        new IndexLeafCellHeader((byte) ((byte[]) columnValue).length),
                        new IndexCellPayload(columnValue, rowId)
                );
                cells.add(cell);
            }
        }
        for (Cell cell : cells) {
            btree.insert(cell);
        }
    }

    public void insert(Object key, int rowId) throws IOException {
        Cell cell = new IndexCell(
                new IndexLeafCellHeader((byte) ((byte[]) key).length),
                new IndexCellPayload(key, rowId)
        );
        btree.insert(cell);
    }

    public void update(Object key, int rowId) throws IOException {
        Cell cell = new IndexCell(
                new IndexLeafCellHeader((byte) ((byte[]) key).length),
                new IndexCellPayload(key, rowId)
        );
        btree.update(cell);
    }

    public int search(Object key) throws IOException {
        Optional<Cell> cellOptional = btree.search(key);
        return cellOptional.map(cell -> cell.cellPayload().getRowId()).orElse(-1);
    }
}
