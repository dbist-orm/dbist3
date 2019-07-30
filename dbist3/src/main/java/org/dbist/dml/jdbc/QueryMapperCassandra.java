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
import org.dbist.metadata.Sequence;

/**
 * @author Minu.Kim
 */
public class QueryMapperCassandra extends AbstractQueryMapper {

    public String getDbType() {
        return DbistConstants.CASSANDRA;
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
        String subsql = null;
        int forUpdateIndex = sql.toLowerCase().lastIndexOf("for update");
        if (forUpdateIndex > -1) {
            subsql = sql.substring(forUpdateIndex - 1);
            sql = sql.substring(0, forUpdateIndex - 1);
        }

        StringBuffer buf = new StringBuffer();
        int pageFromIndex = pagination ? (pageIndex > 0 ? (pageIndex - 1) : 0) * pageSize : 0;
        int offset = pageFromIndex + firstResultIndex;
        long limit = 0;
        if (pageSize > 0) {
            limit = pageSize - firstResultIndex;
            if (maxResultSize > 0)
                limit = Math.min(limit, maxResultSize);
        } else if (maxResultSize > 0) {
            limit = maxResultSize;
        } else if (limit == 0) {
            limit = Long.MAX_VALUE;
        }
        buf.append(sql);
        if (offset > 0 && limit > 0) {
            _paramMap.put("__offset", offset);
            _paramMap.put("__limit", limit);
            buf.append(" limit :__limit offset :__offset");
        } else if (limit > 0) {
            _paramMap.put("__limit", limit);
            buf.append(" limit :__limit");
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
            buf.append(columnName).append(" = true");

        } else if (operator.equalsIgnoreCase("is not true")) {
            buf.append(columnName).append(" != true");

        } else if (operator.equalsIgnoreCase("is false")) {
            buf.append(columnName).append(" = false");

        } else if (operator.equalsIgnoreCase("is not false")) {
            buf.append(columnName).append(" != false");
        }
    }

    public String toNextval(Sequence sequence) {
        if (sequence.getName() == null || sequence.isAutoIncrement())
            return null;
        return "nextval('" + sequence.getName() + "')";
    }

    public String getFunctionLowerCase() {
        return "lower";
    }

    public String getQueryCountTable() {
        return "select count(*) from system_schema.tables where keyspace_name = '${domain}' and table_name = ?";
    }

    public String getQueryPkColumnNames() {
        return "select column_name from system_schema.columns where keyspace_name = '${domain}' and table_name = ? and kind = 'partition_key' allow filtering";
    }

    public String getQueryColumns() {
        return "select column_name as name, type as datatype from system_schema.columns where keyspace_name = '${domain}' and table_name = ?";

    }

    public String getQueryColumn() {
        return "select column_name as name, type as datatype from system_schema.columns where keyspace_name = '${domain}' and table_name = ? and column_name = ?";
    }

    public String getQueryCountIdentity() {
        return "";
    }

    public String getQueryCountSequence() {
        return "";//"select count(*) from information_schema.sequences where lower(sequence_schema) = '${domain}' and lower(sequence_name) = ?";
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

    @Override
    public String toWithNoLock(Lock lock) {
        // TODO Auto-generated method stub
        return null;
    }
}