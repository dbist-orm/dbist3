/**
 * Copyright 2011-2013 the original author or authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dbist.metadata;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.dbist.dml.jdbc.QueryMapper;
import org.dbist.exception.DbistRuntimeException;
import org.dbist.util.ValueGenerator;

import net.sf.common.util.ValueUtils;

/**
 * @author Steve M. Jung
 * @since 2011. 7. 10 (version 0.0.1)
 */
public class Table {
//	@Deprecated
//	public static final String DBTYPE_MYSQL = "mysql";
//	@Deprecated
//	public static final String DBTYPE_ORACLE = "oracle";
//	@Deprecated
//	public static final String DBTYPE_SQLSERVER = "sqlserver";
//	@Deprecated
//	public static final String DBTYPE_DB2 = "db2";

    private String dbType;
    private String domain;
    private String name;
    private String type;
    private Class<?> clazz;
    private boolean containsLinkedTable;
    private boolean reservedWordTolerated;
    private List<String> pkColumnNameList;
    private String[] pkFieldNames;
    private List<String> titleColumnNameList;
    private List<String> listedColumnNameList;
    private List<Column> columnList = new ArrayList<Column>();
    private QueryMapper queryMapper;
    private String insertSql;
    private String updateSql;
    private String deleteSql;
    private Map<Field, ValueGenerator> valueGeneratorByFieldMap = new HashMap<Field, ValueGenerator>();

    public String getDbType() {
        return dbType;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Class<?> getClazz() {
        return clazz;
    }

    public void setClazz(Class<?> clazz) {
        this.clazz = clazz;
    }

    public boolean containsLinkedTable() {
        return containsLinkedTable;
    }

    public void setContainsLinkedTable(boolean containsLinkedTable) {
        this.containsLinkedTable = containsLinkedTable;
    }

    public boolean isReservedWordTolerated() {
        return reservedWordTolerated;
    }

    public void setReservedWordTolerated(boolean reservedWordTolerated) {
        this.reservedWordTolerated = reservedWordTolerated;
    }

    public List<String> getPkColumnNameList() {
        if (ValueUtils.isEmpty(pkColumnNameList) && !ValueUtils.isEmpty(pkFieldNames)) {
            synchronized (this) {
                if (ValueUtils.isEmpty(pkColumnNameList) && !ValueUtils.isEmpty(pkFieldNames)) {
                    List<String> list = new ArrayList<String>();
                    for (String fieldName : pkFieldNames) {
                        String name = toColumnName(fieldName);
                        if (ValueUtils.isEmpty(name))
                            continue;
                        list.add(name);
                    }
                    pkColumnNameList = list;
                }
            }
        }
        return pkColumnNameList;
    }

    public void setPkColumnNameList(List<String> pkColumnNameList) {
        this.pkColumnNameList = pkColumnNameList;
    }

    public boolean isPkColmnName(String name) {
        return getPkColumnNameList().contains(name);
    }

    public String[] getPkFieldNames() {
        populate();
        return pkFieldNames;
    }

    public void setPkFieldNames(String[] pkFieldNames) {
        this.pkFieldNames = pkFieldNames;
    }

    public boolean isPkFieldName(String name) {
        String columnName = toColumnName(name);
        return columnName != null && isPkColmnName(columnName);
    }

    public List<String> getTitleColumnNameList() {
        populate();
        return titleColumnNameList;
    }

    public List<String> getListedColumnNameList() {
        populate();
        return listedColumnNameList;
    }

    private void populate() {
        if (this.titleColumnNameList != null)
            return;
        synchronized (this) {
            if (this.titleColumnNameList != null)
                return;

            List<String> titleColumnNameList = new ArrayList<String>(0);
            listedColumnNameList = new ArrayList<String>(0);
            String titleCandidate = null;
            for (Column column : this.columnList) {
                if (column.isTitle()) {
                    titleColumnNameList.add(column.getName());
                } else if (column.isListed()) {
                    listedColumnNameList.add(column.getName());
                } else if (!column.isPrimaryKey() && titleCandidate == null) {
                    titleCandidate = column.getName();
                }
            }
            if (titleColumnNameList.isEmpty() && titleCandidate != null)
                titleColumnNameList.add(titleCandidate);
            this.titleColumnNameList = titleColumnNameList;

            if (ValueUtils.isEmpty(pkFieldNames)) {
                List<String> pkFieldNameList = new ArrayList<String>();
                for (String columnName : pkColumnNameList)
                    pkFieldNameList.add(toFieldName(columnName));
                pkFieldNames = pkFieldNameList.toArray(new String[pkFieldNameList.size()]);
            }
        }
    }

    public List<Column> getColumnList() {
        return columnList;
    }

    public Column addColumn(Column column) {
        this.columnList.add(column);
        return column;
    }

    private Map<String, Column> columnMap;

    public Column getColumn(String name) {
        ValueUtils.assertNotNull("name", name);
        if (this.columnMap == null) {
            synchronized (this) {
                if (this.columnMap == null) {
                    Map<String, Column> columnMap = new HashMap<String, Column>(this.columnList.size());
                    for (Column column : this.columnList)
                        columnMap.put(column.getName(), column);
                    this.columnMap = columnMap;
                }
            }
        }
        return columnMap.get(name.toLowerCase());
    }

    public Column getColumnByFieldName(String fieldName) {
        String columnName = toColumnName(fieldName);
        return columnName == null ? null : getColumn(columnName);
    }

    public Field getField(String name) {
        Column column = getColumnByFieldName(name);
        return column == null ? null : column.getField();
    }

    public Field getFieldByColumnName(String columnName) {
        Column column = getColumn(columnName);
        return column == null ? null : column.getField();
    }

    private Map<String, String> fieldNameColumNameMap;

    public String toColumnName(String fieldName) {
        if (fieldNameColumNameMap == null || fieldNameColumNameMap.get(fieldName) == null) {
            synchronized (this) {
                if (fieldNameColumNameMap == null || fieldNameColumNameMap.get(fieldName) == null) {
                    Map<String, String> fieldNameColumnNameMap = new HashMap<String, String>(this.columnList.size());
                    for (Column column : this.columnList)
                        fieldNameColumnNameMap.put(column.getField().getName(), column.getName());
                    this.fieldNameColumNameMap = fieldNameColumnNameMap;
                }
            }
        }
        return fieldNameColumNameMap.get(fieldName);
    }

    public String toFieldName(String columnName) {
        Column column = getColumn(columnName);
        return column == null ? null : column.getField().getName();
    }

    public void setQueryMapper(QueryMapper queryMapper) {
        this.queryMapper = queryMapper;
    }

    public StringBuffer appendName(StringBuffer buf, String name) {
        buf.append(reservedWordTolerated ? queryMapper.toReservedWordEscapedName(name) : name);
        return buf;
    }

    public String getInsertSql(String... fieldNames) {
        // Insert all fields
        if (ValueUtils.isEmpty(fieldNames)) {
            if (insertSql != null)
                return insertSql;
            synchronized (this) {
                if (insertSql != null)
                    return insertSql;
                StringBuffer buf = new StringBuffer("insert into ").append(getDomain()).append(".");
                appendName(buf, getName()).append("(");
                StringBuffer valuesBuf = new StringBuffer(" values(");
                int i = 0;
                for (Column column : getColumnList()) {
                    if (column.getRelation() != null || (column.getSequence() != null && column.getSequence().isAutoIncrement()))
                        continue;
                    if (column.getSequence() == null || ValueUtils.isEmpty(column.getSequence().getName())) {
                        buf.append(i == 0 ? "" : ", ");
                        appendName(buf, column.getName());
                        valuesBuf.append(i == 0 ? ":" : ", :").append(column.getField().getName());
                    } else {
                        String str = queryMapper.toNextval(column.getSequence());
                        if (ValueUtils.isEmpty(str))
                            continue;
                        buf.append(i == 0 ? "" : ", ");
                        appendName(buf, column.getName());
                        valuesBuf.append(i == 0 ? "" : ", ").append(str);
                    }
                    i++;
                }
                buf.append(")");
                valuesBuf.append(")");
                buf.append(valuesBuf);

                insertSql = buf.toString();
            }
            return insertSql;
        }

        // Insert some fields
        StringBuffer buf = new StringBuffer("insert into ").append(getDomain()).append(".");
        appendName(buf, getName()).append("(");
        StringBuffer valuesBuf = new StringBuffer(" values(");
        int i = 0;
        List<String> pkFieldNameList = ValueUtils.toList(getPkFieldNames());
        for (String fieldName : pkFieldNameList) {
            Column column = getColumnByFieldName(fieldName);
            if (column.getRelation() != null || (column.getSequence() != null && column.getSequence().isAutoIncrement()))
                continue;
            if (column.getSequence() == null || ValueUtils.isEmpty(column.getSequence().getName())) {
                buf.append(i == 0 ? "" : ", ");
                appendName(buf, column.getName());
                valuesBuf.append(i == 0 ? ":" : ", :").append(fieldName);
            } else {
                String str = queryMapper.toNextval(column.getSequence());
                if (ValueUtils.isEmpty(str))
                    continue;
                buf.append(i == 0 ? "" : ", ");
                appendName(buf, column.getName());
                valuesBuf.append(i == 0 ? "" : ", ").append(str);
            }
            i++;
        }
        for (String fieldName : fieldNames) {
            if (pkFieldNameList.contains(fieldName))
                continue;
            Column column = getColumnByFieldName(fieldName);
            if (column == null)
                throw new DbistRuntimeException("Invalid fieldName: " + getClazz().getName() + "." + fieldName);
            if (column.getRelation() != null || (column.getSequence() != null && column.getSequence().isAutoIncrement()))
                continue;
            if (column.getSequence() == null || ValueUtils.isEmpty(column.getSequence().getName())) {
                buf.append(i == 0 ? "" : ", ");
                appendName(buf, column.getName());
                valuesBuf.append(i == 0 ? ":" : ", :").append(fieldName);
            } else {
                String str = queryMapper.toNextval(column.getSequence());
                if (ValueUtils.isEmpty(str))
                    continue;
                buf.append(i == 0 ? "" : ", ");
                appendName(buf, column.getName());
                valuesBuf.append(i == 0 ? "" : ", ").append(str);
            }
            i++;
        }
        buf.append(")");
        valuesBuf.append(")");
        buf.append(valuesBuf);

        return buf.toString();
    }

    public String getUpdateSql(String... fieldNames) {
        // Update all fields
        if (ValueUtils.isEmpty(fieldNames)) {
            if (updateSql != null)
                return updateSql;
            synchronized (this) {
                if (updateSql != null)
                    return updateSql;
                StringBuffer buf = new StringBuffer("update ").append(getDomain()).append(".");
                appendName(buf, getName()).append(" set ");
                StringBuffer whereBuf = new StringBuffer();
                int i = 0;
                int j = 0;
                for (Column column : getColumnList()) {
                    if (column.getRelation() != null)
                        continue;
                    if (column.isPrimaryKey()) {
                        whereBuf.append(j++ == 0 ? " where " : " and ");
                        appendName(whereBuf, column.getName()).append(" = ").append(":").append(column.getField().getName());
                        continue;
                    }
                    buf.append(i++ == 0 ? "" : ", ");
                    appendName(buf, column.getName()).append(" = :").append(column.getField().getName());
                }
                updateSql = buf.append(whereBuf).toString();
            }
            return updateSql;
        }

        // Update some fields
        StringBuffer buf = new StringBuffer("update ").append(getDomain()).append(".");
        appendName(buf, getName()).append(" set ");
        int i = 0;
        int j = 0;
        for (String fieldName : fieldNames) {
            Column column = getColumnByFieldName(fieldName);
            if (column == null)
                throw new DbistRuntimeException("Invalid fieldName: " + getClazz().getName() + "." + fieldName);
            if (column.isPrimaryKey())
                throw new DbistRuntimeException("Updating primary key is not supported. " + getDomain() + "." + getName() + getPkColumnNameList());
            buf.append(i++ == 0 ? "" : ", ");
            appendName(buf, toColumnName(fieldName)).append(" = :").append(fieldName);
        }
        StringBuffer whereBuf = new StringBuffer();
        for (String columnName : getPkColumnNameList())
            whereBuf.append(j++ == 0 ? " where " : " and ").append(columnName).append(" = ").append(":").append(toFieldName(columnName));
        return buf.append(whereBuf).toString();
    }

    public String getDeleteSql() {
        if (deleteSql != null)
            return deleteSql;
        synchronized (this) {
            if (deleteSql != null)
                return deleteSql;
            StringBuffer buf = new StringBuffer("delete from ").append(getDomain()).append(".");
            appendName(buf, getName());
            int i = 0;
            for (String columnName : getPkColumnNameList())
                buf.append(i++ == 0 ? " where " : " and ").append(columnName).append(" = :").append(getColumn(columnName).getField().getName());
            deleteSql = buf.toString();
        }
        return deleteSql;
    }

    public Map<Field, ValueGenerator> getValueGeneratorByFieldMap() {
        return valueGeneratorByFieldMap;
    }

    public void setValueGeneratorByFieldMap(Map<Field, ValueGenerator> valueGeneratorByFieldMap) {
        this.valueGeneratorByFieldMap = valueGeneratorByFieldMap;
    }
}
