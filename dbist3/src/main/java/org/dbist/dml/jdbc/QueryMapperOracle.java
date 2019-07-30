/**
 * Copyright 2011-2014 the original author or authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dbist.dml.jdbc;

import java.util.Map;
import java.util.StringJoiner;

import org.dbist.DbistConstants;
import org.dbist.dml.Lock;

/**
 * @author Steve M. Jung
 * @since 2013. 9. 7. (version 2.0.3)
 */
public class QueryMapperOracle extends AbstractQueryMapper {

    public String getDbType() {
        return DbistConstants.ORACLE;
    }

    public boolean isSupportedPaginationQuery() {
        return true;
    }

    public boolean isSupportedLockTimeout() {
        return true;
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
        String subsql = null;
        int forUpdateIndex = sql.toLowerCase().lastIndexOf("for update");
        if (forUpdateIndex > -1) {
            subsql = sql.substring(forUpdateIndex - 1);
            sql = sql.substring(0, forUpdateIndex - 1);
        }

        StringBuffer buf = new StringBuffer();
        int pageFromIndex = pagination ? (pageIndex > 0 ? (pageIndex - 1) : 0) * pageSize : 0;
        int fromIndex = pageFromIndex + firstResultIndex;
        int toIndex = 0;
        if (pageSize > 0) {
            toIndex = pageFromIndex + pageSize;
            if (maxResultSize > 0)
                toIndex = Math.min(toIndex, fromIndex + maxResultSize);
        } else if (maxResultSize > 0) {
            toIndex = fromIndex + maxResultSize;
        }
        if (fromIndex > 0 && toIndex > 0) {
            _paramMap.put("__fromIndex", fromIndex);
            _paramMap.put("__toIndex", toIndex);
            buf.append("select * from (select pagetbl_.*, rownum rownum_ from (").append(sql)
                .append(") pagetbl_ where rownum <= :__toIndex order by rownum) where rownum_ > :__fromIndex");
        } else if (toIndex > 0) {
            _paramMap.put("__toIndex", toIndex);
            buf.append("select * from (").append(sql).append(") where rownum <= :__toIndex order by rownum");
        } else if (fromIndex > 0) {
            _paramMap.put("__fromIndex", fromIndex);
            buf.append("select * from (").append(sql).append(") where rownum > :__fromIndex order by rownum");
        } else {
            buf.append(sql);
        }

        if (subsql != null)
            buf.append(subsql);
        return buf.toString();
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

    public String getFunctionLowerCase() {
        return "lower";
    }

    public String getQueryCountTable() {
        return "select count(*) from all_tables where lower(owner) = '${domain}' and lower(table_name) = ?";
    }

    public String getQueryCountView() {
        return "select count(*) from all_views where lower(owner) = '${domain}' and lower(view_name) = ?";
    }

    public String getQueryPkColumnNames() {
        return "select lower(conscol.column_name) name from all_constraints cons, all_cons_columns conscol"
            + " where cons.constraint_name = conscol.constraint_name and cons.owner = conscol.owner and lower(conscol.owner) = '${domain}' and lower(conscol.table_name) = ? and cons.constraint_type = 'P' order by conscol.position";
    }

    public String getQueryColumns() {
        return "select lower(column_name) name, data_type datatype, data_length length, nullable nullable from all_tab_columns where lower(owner) = '${domain}' and lower(table_name) = ?";
    }

    public String getQueryColumn() {
        return "select lower(column_name) name, data_type datatype, data_length length, nullable nullable from all_tab_columns where lower(owner) = '${domain}' and lower(table_name) = ? and lower(column_name) = ?";
    }

    public String getQueryCountIdentity() {
        return "";
    }

    public String getQueryCountSequence() {
        return "select count(*) from all_sequences where lower(sequence_owner) = '${domain}' and lower(sequence_name) = ?";
    }

    @Override
    public String procedureParameters(String name) {
        StringJoiner sql = new StringJoiner("\n");
        sql.add("SELECT ARGUMENT_NAME AS PARAMETER_NAME,");
        sql.add("       DATA_TYPE,");
        sql.add("       IN_OUT,");
        sql.add("       SEQUENCE");
        sql.add("FROM all_arguments");
        sql.add("WHERE object_id =");
        sql.add("    (SELECT object_id");
        sql.add("     FROM all_objects");
        sql.add("     WHERE OWNER in (:domain)");
        sql.add("       AND UPPER(object_name) = UPPER(:name)");
        sql.add("       AND object_type = 'PROCEDURE')");
        sql.add("   AND in_out = 'IN'");
        sql.add("	ORDER BY object_Name, overload, sequence");
        return sql.toString();
    }

    @Override
    public String callProcedure(String name, Map<String, ?> paramMap) {
        StringBuilder params = new StringBuilder();
        paramMap.forEach((k, v) -> params.append(":").append(k).append(","));

        StringBuilder appender = new StringBuilder();
        appender.append("EXECUTE ").append(name);
        appender.append("(");

        if (params.length() > 0) {
            appender.append(params.substring(0, params.lastIndexOf(",")));
        }

        appender.append(")");

        return appender.toString();
    }

    @Override
    public String toWithNoLock(Lock lock) {
        // TODO Auto-generated method stub
        return null;
    }
}