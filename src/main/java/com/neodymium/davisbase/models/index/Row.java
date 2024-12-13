package com.neodymium.davisbase.models.table;

import com.neodymium.davisbase.constants.enums.DataTypes;
import com.neodymium.davisbase.models.Cell;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public record Row(Map<Column, Object> data) {

    public static Row fromCell(Cell cell, Map<String, Column> schema) {
         Map<Column,Object > rowData=new HashMap<>();
         ByteBuffer buffer=ByteBuffer.wrap(cell.cellPayload().serialize());
         for(Column column:schema.values()){
             switch(column.dataType()){
                 case NULL->rowData.put(column,null);
                 case TINYINT,YEAR->rowData.put(column,buffer.get());
                 case SMALLINT->rowData.put(column,buffer.getShort());
                 case INT->rowData.put(column,buffer.getInt());
                 case BIGINT,LONG,DATE,DATETIME->rowData.put(column,buffer.getLong());
                 case FLOAT->rowData.put(column,buffer.getFloat());
                 case DOUBLE->rowData.put(column,buffer.getDouble());
                 default->{
                     byte[] textBytes=new byte[column.dataType().getSize()];
                     buffer.get(textBytes);
                     rowData.put(column,new String(textBytes));
                 }
             }
         }
         return new Row(rowData);
     }

     public Cell cellFromRow(){
         int estimatedSize=data.keySet().stream()
                 .mapToInt(Row::estimateSize)
                 .sum();

         int noOfColumns=data.size();
         List<DataTypes> dataTypes=data.keySet().stream()
                 .map(Column::dataType)
                 .toList();

         ByteBuffer bodyBuffer=ByteBuffer.allocate(estimatedSize);

         for(Map.Entry<Column,Object > entry:data.entrySet()){
             Column column=entry.getKey();
             Object value=entry.getValue();

             switch(column.dataType()){
                 case NULL->bodyBuffer.put((byte )0);
                 case TINYINT,YEAR->bodyBuffer.put(((Number)value).byteValue());
                 case SMALLINT->bodyBuffer.putShort(((Number)value).shortValue());
                 case INT->bodyBuffer.putInt((Integer)value);
                 case BIGINT,LONG ,DATE,DATETIME->bodyBuffer.putLong(((Number)value).longValue());
                 case FLOAT->bodyBuffer.putFloat((Float)value);
                 case DOUBLE->bodyBuffer.putDouble((Double)value);
                 default->{
                     String strValue=(String)value;
                     bodyBuffer.put(strValue.getBytes());
                 }
             }
         }

         byte[] body=bodyBuffer.array();

         CellHeader cellHeader=new TableLeafCellHeader((short )estimatedSize,id);
         CellPayload cellPayload=new TableCellPayload((byte )0,(byte )noOfColumns,dataTypes ,body);

         byte[] cellHeaderInBytes=cellHeader.serialize();
         byte[] cellPayloadInBytes=cellPayload.serialize();
         ByteBuffer payloadBuffer=ByteBuffer.allocate(cellHeaderInBytes.length+cellPayloadInBytes.length);
         payloadBuffer.put(cellHeaderInBytes );
         payloadBuffer.put(cellPayloadInBytes );
         byte[] payload=payloadBuffer.array();
         return TableCell.deserialize(payload ,PageTypes.LEAF );
     }
}
