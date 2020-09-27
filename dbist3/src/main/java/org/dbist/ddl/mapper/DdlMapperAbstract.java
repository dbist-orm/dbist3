package org.dbist.ddl.mapper;

import java.util.ArrayList;
import java.util.List;

import org.dbist.DbistConstants;

public abstract class DdlMapperAbstract {

    /**
     * 실수형 여부 반환.
     */
    public boolean isRealType(String fieldType) {
        List<String> realTypeList = new ArrayList<>();
        realTypeList.add(DbistConstants.FIELD_TYPE_FLOAT.toUpperCase());
        realTypeList.add(DbistConstants.FIELD_TYPE_DOUBLE.toUpperCase());
        realTypeList.add(DbistConstants.FIELD_TYPE_DECIMAL.toUpperCase());
        return realTypeList.contains(fieldType.toUpperCase());
    }

    /**
     * 숫자 타입 여부 확인.
     */
    public boolean isNumberType(String fieldType) {
        List<String> numTypeList = new ArrayList<>();
        numTypeList.add(DbistConstants.FIELD_TYPE_FLOAT.toUpperCase());
        numTypeList.add(DbistConstants.FIELD_TYPE_DOUBLE.toUpperCase());
        numTypeList.add(DbistConstants.FIELD_TYPE_DECIMAL.toUpperCase());
        numTypeList.add(DbistConstants.FIELD_TYPE_INT.toUpperCase());
        numTypeList.add(DbistConstants.FIELD_TYPE_INTEGER.toUpperCase());
        numTypeList.add(DbistConstants.FIELD_TYPE_LONG.toUpperCase());
        numTypeList.add(DbistConstants.FIELD_TYPE_SHORT.toUpperCase());
        return numTypeList.contains(fieldType.toUpperCase());
    }
}