package com.neodymium.davisbase.models;

import com.neodymium.davisbase.constants.enums.DataTypes;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class TableRecord implements Record {

    byte[] deletionFlag;
    int noOfColumns;
    List<DataTypes> dataTypes;
    List<byte[]> body;

    @Override
    public void delete() {
        if (deletionFlag == null || deletionFlag.length == 0) {
            deletionFlag = new byte[1];
        }
        deletionFlag[0] = 1;
    }

    @Override
    public boolean exists() {
        if (deletionFlag == null || deletionFlag.length == 0) {
            return true;
        }
        return deletionFlag[0] == 0;
    }

}
