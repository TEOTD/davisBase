package com.neodymium.davisbase.config;

import com.neodymium.davisbase.constants.enums.Constraints;
import com.neodymium.davisbase.constants.enums.DataTypes;
import com.neodymium.davisbase.error.DavisBaseException;
import com.neodymium.davisbase.models.table.Column;
import com.neodymium.davisbase.models.table.Table;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.ObjectUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.neodymium.davisbase.constants.Constants.COLUMN_CATALOG_NAME;
import static com.neodymium.davisbase.constants.Constants.TABLE_CATALOG_NAME;

@Configuration
public class CatalogInitializer {

    CatalogInitializer() {
        try {
            Table davisbaseTables = new Table(TABLE_CATALOG_NAME);
            if (!davisbaseTables.exists()) {
                List<Column> tableColumnSchema = List.of(
                        new Column("rowid", DataTypes.INT.getTypeCode(), null),
                        new Column("table_name", DataTypes.TEXT.getTypeCode(), null),
                        new Column("record_count", DataTypes.INT.getTypeCode(), null),
                        new Column("avg_length", DataTypes.SMALLINT.getTypeCode(), null),
                        new Column("root_page", DataTypes.SMALLINT.getTypeCode(), null)
                );
                davisbaseTables.create(tableColumnSchema);

                Table davisbaseColumns = new Table(COLUMN_CATALOG_NAME);
                if (!davisbaseColumns.exists()) {
                    List<Column> columns = List.of(
                            new Column("rowid", DataTypes.INT.getTypeCode(), null),
                            new Column("column_name", DataTypes.TEXT.getTypeCode(), null),
                            new Column("table_rowid", DataTypes.INT.getTypeCode(), null),
                            new Column("table_name", DataTypes.TEXT.getTypeCode(), null),
                            new Column("data_type", DataTypes.TEXT.getTypeCode(), null),
                            new Column("ordinal_position", DataTypes.TINYINT.getTypeCode(), null),
                            new Column("constraints", DataTypes.TEXT.getTypeCode(), null)
                    );
                    davisbaseColumns.create(columns);

                    Map<String, Map<String, Object>> tableRecords = new HashMap();
                    Map<String, Object> tableRecord1 = new HashMap<>();
                    tableRecord1.put("rowid", 0);
                    tableRecord1.put("table_name", TABLE_CATALOG_NAME);
                    tableRecord1.put("record_count", 0);
                    tableRecord1.put("avg_length", 0);
                    tableRecord1.put("root_page", 0);
                    tableRecords.put(TABLE_CATALOG_NAME, tableRecord1);

                    Map<String, Object> tableRecord2 = new HashMap<>();
                    tableRecord2.put("rowid", 1);
                    tableRecord2.put("table_name", COLUMN_CATALOG_NAME);
                    tableRecord2.put("record_count", 0);
                    tableRecord2.put("avg_length", 0);
                    tableRecord2.put("root_page", 0);
                    tableRecords.put(COLUMN_CATALOG_NAME, tableRecord2);

                    int ordinalPosition = 1;
                    int rowId = 1;
                    for (Map.Entry<String, Map<String, Object>> tableRecord : tableRecords.entrySet()) {
                        davisbaseTables.insert(tableRecord.getValue());
                        if (TABLE_CATALOG_NAME.equals(tableRecord.getKey())) {
                            for (Column column : tableColumnSchema) {
                                Map<String, Object> columnRecord = new HashMap<>();
                                columnRecord.put("rowid", rowId++);
                                columnRecord.put("column_name", column.name());
                                columnRecord.put("table_rowid", tableRecord.getValue().get("rowid"));
                                columnRecord.put("table_name", tableRecord.getValue().get("table_name"));
                                columnRecord.put("data_type", DataTypes.getFromTypeCode(column.typeCode()).getTypeName());
                                columnRecord.put("ordinal_position", ordinalPosition++);
                                if (!ObjectUtils.isEmpty(column.constraints())) {
                                    columnRecord.put("constraints", String.join(",", column.constraints().stream()
                                            .map(Constraints::getValue)
                                            .toList()));
                                } else {
                                    columnRecord.put("constraints", null);
                                }
                                davisbaseColumns.insert(columnRecord);
                            }
                        } else {
                            for (Column column : columns) {
                                Map<String, Object> columnRecord = new HashMap<>();
                                columnRecord.put("rowid", rowId++);
                                columnRecord.put("column_name", column.name());
                                columnRecord.put("table_rowid", tableRecord.getValue().get("rowid"));
                                columnRecord.put("table_name", tableRecord.getValue().get("table_name"));
                                columnRecord.put("data_type", DataTypes.getFromTypeCode(column.typeCode()).getTypeName());
                                columnRecord.put("ordinal_position", ordinalPosition++);
                                if (!ObjectUtils.isEmpty(column.constraints())) {
                                    columnRecord.put("constraints", String.join(",", column.constraints().stream()
                                            .map(Constraints::getValue)
                                            .toList()));
                                } else {
                                    columnRecord.put("constraints", null);
                                }
                                davisbaseColumns.insert(columnRecord);
                            }
                        }

                    }
                }
            }
        } catch (Exception exception) {
            throw new DavisBaseException(exception);
        }
    }
}