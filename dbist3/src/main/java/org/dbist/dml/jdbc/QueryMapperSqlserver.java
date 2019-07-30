/**
 * Copyright 2011-2014 the original author or authors.
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
package org.dbist.dml.jdbc;

import java.util.Map;

import org.dbist.DbistConstants;
import org.dbist.dml.Lock;

/**
 * @author Steve M. Jung
 * @since 2013. 9. 7. (version 2.0.3)
 */
public class QueryMapperSqlserver extends AbstractQueryMapper {

    public String getDbType() {
        return DbistConstants.SQLSERVER;
    }

    public boolean isSupportedPaginationQuery() {
        return true;
    }

    public boolean isSupportedLockTimeout() {
        return false;
    }

    public String applyPagination(String sql, Map<String, ?> paramMap, int pageIndex, int pageSize, int firstResultIndex, int maxResultSize) {
        boolean pagination = pageIndex >= 0 && pageSize > 0;
        boolean fragment = firstResultIndex > 0 || maxResultSize > 0;

        if (!pagination && !fragment)
            return sql;

        if (!pagination) {
            pageIndex = 0;
            pageSize = 0;
        }

        if (firstResultIndex < 0)
            firstResultIndex = 0;

        if (maxResultSize < 0)
            maxResultSize = 0;

        @SuppressWarnings("unchecked")
        Map<String, Object> _paramMap = (Map<String, Object>) paramMap;

        int pageFromIndex = pagination ? (pageIndex > 0 ? (pageIndex - 1) : 0) * pageSize : 0;
        int offset = pageFromIndex + firstResultIndex;
        long limit = 0;

        if (pageSize > 0) {
            limit = pageSize - firstResultIndex;
            if (maxResultSize > 0) {
                limit = Math.min(limit, maxResultSize);
            }
        } else if (maxResultSize > 0) {
            limit = maxResultSize;
        } else if (limit == 0) {
            limit = Long.MAX_VALUE;
        }

        if (limit > 0) {
            _paramMap.put("__offset", offset);
            _paramMap.put("__limit", limit);
            sql = sql + " OFFSET :__offset ROWS FETCH NEXT :__limit ROWS ONLY";
        }

        return sql;
    }

    /**
     * Append Empty Value Condition Where Statement
     *
     * @param buf
     * @param columnName
     * @param operator
     */
    public void appendEmptyValueCondition(StringBuffer buf, String columnName, String operator) {
        if (operator.equalsIgnoreCase("is null") || operator.equalsIgnoreCase("is not null")) {
            buf.append(columnName).append(" ").append(operator).append(" ");

        } else if (operator.equalsIgnoreCase("is persent")) {
            buf.append("(").append(columnName).append(" is not null and ").append(columnName).append(" <> '')");

        } else if (operator.equalsIgnoreCase("is blank")) {
            buf.append("(").append(columnName).append(" is null or ").append(columnName).append(" = '')");

        } else if (operator.equalsIgnoreCase("is empty numeric id")) {
            buf.append("(").append(columnName).append(" is null or ").append(columnName).append(" = 0)");

        } else if (operator.equalsIgnoreCase("is true")) {
            buf.append(columnName).append(" = 1");

        } else if (operator.equalsIgnoreCase("is not true")) {
            buf.append("(").append(columnName).append(" is null or ").append(columnName).append(" = 0)");

        } else if (operator.equalsIgnoreCase("is false")) {
            buf.append(columnName).append(" = 0");

        } else if (operator.equalsIgnoreCase("is not false")) {
            buf.append("(").append(columnName).append(" is null or ").append(columnName).append(" = 1)");
        }
    }

    public String toWithLock(Lock lock) {
        return "with (updlock, rowlock)";
    }

    public String toWithNoLock(Lock lock) {
        return "with (nolock)";
    }

    @Override
    public String toForUpdate(Lock lock) {
        return "";
    }

    public String getFunctionLowerCase() {
        return "lower";
    }

    public String getQueryCountTable() {
        return "select count(*) from information_schema.tables where lower(table_schema) = '${domain}' and lower(table_name) = ?";
    }

    public String getQueryPkColumnNames() {
        return "select lower(col.column_name) name from information_schema.table_constraints tbl, information_schema.constraint_column_usage col, information_schema.columns cols"
            + " where lower(tbl.table_schema) = '${domain}' and lower(tbl.table_name) = ? and tbl.constraint_type = 'PRIMARY KEY'"
            + " and col.constraint_name = tbl.constraint_name and col.table_name = tbl.table_name and col.table_schema = tbl.table_schema and col.table_schema = tbl.table_schema"
            + " and col.table_name = cols.table_name and col.table_schema = cols.table_schema and col.table_schema = cols.table_schema and col.column_name = cols.column_name"
            + " order by cols.ordinal_position";
    }

    public String getQueryColumns() {
        return "select lower(column_name) name, data_type datatype, character_maximum_length length, is_nullable nullable from information_schema.columns where lower(table_schema) = '${domain}' and lower(table_name) = ? order by ordinal_position";
    }

    public String getQueryColumn() {
        return "select lower(column_name) name, data_type datatype, character_maximum_length length, is_nullable nullable from information_schema.columns where lower(table_schema) = '${domain}' and lower(table_name) = ? and lower(column_name) = ?";
    }

    public String getQueryCountIdentity() {
        return "";
    }

    public String getQueryCountSequence() {
        return "";
    }

    public char getReservedWordEscapingBraceOpen() {
        return '[';
    }

    public char getReservedWordEscapingBraceClose() {
        return ']';
    }

    @Override
    public String procedureParameters(String name) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String callProcedure(String name, Map<String, ?> paramMap) {
        // TODO Auto-generated method stub
        return null;
    }
}