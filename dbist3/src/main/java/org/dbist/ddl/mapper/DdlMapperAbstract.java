package org.dbist.ddl.mapper;

import java.util.ArrayList;
import java.util.List;

import org.dbist.DbistConfigConstants;
import org.dbist.DbistConstants;
import org.springframework.core.env.Environment;

import net.sf.common.util.BeanUtils;

public abstract class DdlMapperAbstract {

    private Environment env;

    private String stringFieldSize;
    private String integerFieldSize;
    private String longFieldSize;
    private String floatFieldSize;
    private String doubleFieldSize;
    private String sortFieldSize;
    private String decimalFieldSize;
    private String timeStampFieldSize;
    private String dateTimeFieldSize;
    private String decimalPoint;

    public String getStringFieldSize() {
        if (stringFieldSize == null) {
            stringFieldSize = getEnv().getProperty(DbistConfigConstants.DBIST_DDL_STRING_SIZE, "255");
        }

        return stringFieldSize;
    }

    public void setStringFieldSize(String stringFieldSize) {
        this.stringFieldSize = stringFieldSize;
    }

    public String getIntegerFieldSize() {
        if (integerFieldSize == null) {
            integerFieldSize = getEnv().getProperty(DbistConfigConstants.DBIST_DDL_INTEGER_SIZE);
        }
        return integerFieldSize;
    }

    public void setIntegerFieldSize(String integerFieldSize) {
        this.integerFieldSize = integerFieldSize;
    }

    public String getLongFieldSize() {
        if (longFieldSize == null) {
            longFieldSize = getEnv().getProperty(DbistConfigConstants.DBIST_DDL_LONG_SIZE);
        }
        return longFieldSize;
    }

    public void setLongFieldSize(String longFieldSize) {
        this.longFieldSize = longFieldSize;
    }

    public String getFloatFieldSize() {
        if (floatFieldSize == null) {
            floatFieldSize = getEnv().getProperty(DbistConfigConstants.DBIST_DDL_FLOAT_SIZE);
        }
        return floatFieldSize;
    }

    public void setFloatFieldSize(String floatFieldSize) {
        this.floatFieldSize = floatFieldSize;
    }

    public String getDoubleFieldSize() {
        if (doubleFieldSize == null) {
            doubleFieldSize = getEnv().getProperty(DbistConfigConstants.DBIST_DDL_DOUBLE_SIZE);
        }
        return doubleFieldSize;
    }

    public void setDoubleFieldSize(String doubleFieldSize) {
        this.doubleFieldSize = doubleFieldSize;
    }

    public String getSortFieldSize() {
        if (sortFieldSize == null) {
            sortFieldSize = getEnv().getProperty(DbistConfigConstants.DBIST_DDL_SHORT_SIZE);
        }
        return sortFieldSize;
    }

    public void setSortFieldSize(String sortFieldSize) {
        this.sortFieldSize = sortFieldSize;
    }

    public String getDecimalFieldSize() {
        if (decimalFieldSize == null) {
            decimalFieldSize = getEnv().getProperty(DbistConfigConstants.DBIST_DDL_INTEGER_SIZE, "15");
        }
        return decimalFieldSize;
    }

    public void setDecimalFieldSize(String decimalFieldSize) {
        this.decimalFieldSize = decimalFieldSize;
    }

    public String getTimeStampFieldSize() {
        if (timeStampFieldSize == null) {
            timeStampFieldSize = getEnv().getProperty(DbistConfigConstants.DBIST_DDL_TIMESTAMP_SIZE, "6");
        }
        return timeStampFieldSize;
    }

    public void setTimeStampFieldSize(String timeStampFieldSize) {
        this.timeStampFieldSize = timeStampFieldSize;
    }

    public String getDateTimeFieldSize() {
        if (dateTimeFieldSize == null) {
            dateTimeFieldSize = getEnv().getProperty(DbistConfigConstants.DBIST_DDL_TIMESTAMP_SIZE, "6");
        }
        return dateTimeFieldSize;
    }

    public void setDateTimeFieldSize(String dateTimeFieldSize) {
        this.dateTimeFieldSize = dateTimeFieldSize;
    }

    public String getDecimalPoint() {
        if (decimalPoint == null) {
            decimalPoint = getEnv().getProperty(DbistConfigConstants.DBIST_DDL_DECIMAL_POINT, "3");
        }
        return decimalPoint;
    }

    public void setDecimalPoint(String decimalPoint) {
        this.decimalPoint = decimalPoint;
    }

    public Environment getEnv() {
        if (env == null) {
            env = BeanUtils.getInstance(DbistConstants.APPLICATION_CONTEXT_NAME).get(Environment.class);
        }
        return env;
    }

    /**
     * 실수형 여부 반환.
     *
     * @param fieldType
     * @return
     */
    public boolean isRealType(String fieldType) {
        List<String> realTypeList = new ArrayList<String>();
        realTypeList.add(DbistConstants.FIELD_TYPE_FLOAT.toUpperCase());
        realTypeList.add(DbistConstants.FIELD_TYPE_DOUBLE.toUpperCase());
        realTypeList.add(DbistConstants.FIELD_TYPE_DECIMAL.toUpperCase());
        return realTypeList.contains(fieldType.toUpperCase());
    }

    /**
     * 숫자 타입 여부 확인.
     *
     * @param fieldType
     * @return
     */
    public boolean isNumberType(String fieldType) {
        List<String> numTypeList = new ArrayList<String>();
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