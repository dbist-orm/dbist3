/**
 * Copyright 2011-2012 the original author or authors.
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
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.dbist.annotation.Relation;

/**
 * @author Steve M. Jung
 * @since 2011. 7. 10 (version 0.0.1)
 */
public class Column {
    public static final String TYPE_TITLE = "title";
    public static final String TYPE_LISTED = "listed";
    public static final String TYPE_PASSWORD = "password";
    public static final String TYPE_TEXT = "text";
    public static final String TYPE_DATETIME = "datetime";

    private String name;
    private Table table;
    private Relation relation;
    private List<Column> columnList;
    private boolean primaryKey;
    private Sequence sequence;
    private String dataType;
    private String type;
    private Integer length;
    private String nullable;
    private Field field;
    private Method getter;
    private Method setter;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public Relation getRelation() {
        return relation;
    }

    public void setRelation(Relation relation) {
        this.relation = relation;
    }

    public List<Column> getColumnList() {
        return columnList;
    }

    public void setColumnList(List<Column> column) {
        this.columnList = column;
    }

    public Column addColumn(Column column) {
        if (this.columnList == null)
            this.columnList = new ArrayList<Column>();
        this.columnList.add(column);
        return column;
    }

    public boolean isPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(boolean primaryKey) {
        this.primaryKey = primaryKey;
    }

    public Sequence getSequence() {
        return sequence;
    }

    public void setSequence(Sequence sequence) {
        this.sequence = sequence;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Integer getLength() {
        return length;
    }

    public void setLength(Integer length) {
        this.length = length;
    }

    public String isNullable() {
        return nullable;
    }

    public void setNullable(String nullable) {
        this.nullable = nullable;
    }

    public boolean isTitle() {
        return TYPE_TITLE.equals(type);
    }

    public boolean isListed() {
        return TYPE_LISTED.equals(type);
    }

    public boolean isPassword() {
        return TYPE_PASSWORD.equals(type);
    }

    public boolean isText() {
        return TYPE_TEXT.equals(type);
    }

    public boolean isDateTime() {
        return TYPE_DATETIME.equals(type);
    }

    public Field getField() {
        return field;
    }

    public void setField(Field field) {
        this.field = field;
    }

    public Method getGetter() {
        return getter;
    }

    public void setGetter(Method getter) {
        this.getter = getter;
    }

    public Method getSetter() {
        return setter;
    }

    public void setSetter(Method setter) {
        this.setter = setter;
    }
}
