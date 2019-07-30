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
package org.dbist.dml.impl;

import java.util.List;
import java.util.Map;

import org.dbist.dml.AbstractDml;
import org.dbist.dml.Dml;
import org.dbist.dml.Page;
import org.dbist.dml.jdbc.QueryMapper;
import org.dbist.metadata.Table;

/**
 * @author Steve M. Jung
 * @since 2011. 6. 2. (version 0.0.1)
 */
public class DmlJdo extends AbstractDml implements Dml {

    public Class<?> getClass(String tableName) {
        // TODO Auto-generated method stub
        return null;
    }

    public Table getTable(Object obj) {
        // TODO Auto-generated method stub
        return null;
    }

    public void insert(Object data) throws Exception {
        // TODO Auto-generated method stub

    }

    public void insertBatch(List<?> list) throws Exception {
        // TODO Auto-generated method stub

    }

    public void insert(Object data, String... fieldNames) throws Exception {
        // TODO Auto-generated method stub

    }

    public void insertBatch(List<?> list, String... fieldNames) throws Exception {
        // TODO Auto-generated method stub

    }

    public void update(Object data) throws Exception {
        // TODO Auto-generated method stub

    }

    public void updateBatch(List<?> list) throws Exception {
        // TODO Auto-generated method stub

    }

    public void update(Object data, String... fieldNames) throws Exception {
        // TODO Auto-generated method stub

    }

    public void updateBatch(List<?> list, String... fieldNames) throws Exception {
        // TODO Auto-generated method stub

    }

    public void delete(Object data) throws Exception {
        // TODO Auto-generated method stub

    }

    public void deleteBatch(List<?> list) throws Exception {
        // TODO Auto-generated method stub

    }

    public int selectSize(Class<?> clazz, Object condition) throws Exception {
        // TODO Auto-generated method stub
        return 0;
    }

    public <T> List<T> selectList(Class<T> clazz, Object condition) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    public <T> List<T> selectListWithLock(Class<T> clazz, Object condition) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    public <T> List<T> selectListByQl(String ql, Map<String, ?> paramMap, Class<T> requiredType, int pageIndex, int pageSize, int firstResultIndex,
                                      int maxResultSize) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    public int selectSizeByQl(String ql, Map<String, ?> paramMap) throws Exception {
        // TODO Auto-generated method stub
        return 0;
    }

    public <T> List<T> selectListByQlPath(String qlPath, Map<String, ?> paramMap, Class<T> requiredType, int pageIndex, int pageSize,
                                          int firstResultIndex, int maxResultSize) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    public <T> Page<T> selectPageByQlPath(String qlPath, Map<String, ?> paramMap, Class<T> requiredType, int pageIndex, int pageSize,
                                          int firstResultIndex, int maxResultSize) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    public int selectSizeByQlPath(String qlPath, Map<String, ?> paramMap) throws Exception {
        // TODO Auto-generated method stub
        return 0;
    }

    public int deleteList(Class<?> clazz, Object condition) throws Exception {
        // TODO Auto-generated method stub
        return 0;
    }

    public int executeByQl(String ql, Map<String, ?> paramMap) throws Exception {
        // TODO Auto-generated method stub
        return 0;
    }

    public int executeByQlPath(String qlPath, Map<String, ?> paramMap) throws Exception {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public QueryMapper getQueryMapper() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void callProcedure(String name) throws Exception {
        // TODO Auto-generated method stub

    }

    @Override
    public void callProcedure(String name, Map<String, ?> paramMap) throws Exception {
        // TODO Auto-generated method stub

    }

    @Override
    public <T> T callReturnProcedure(String name, Map<String, ?> paramMap, Class<T> requiredType) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <T> List<T> callReturnListProcedure(String name, Map<String, ?> paramMap, Class<T> requiredType) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void callProcedureBySql(String sql) throws Exception {
        // TODO Auto-generated method stub

    }

    @Override
    public void callProcedureBySql(String sql, Map<String, ?> paramMap) throws Exception {
        // TODO Auto-generated method stub

    }

    @Override
    public <T> T callReturnProcedureBySql(String sql, Map<String, ?> paramMap, Class<T> requiredType) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <T> List<T> callReturnListProcedureBySql(String sql, Map<String, ?> paramMap, Class<T> requiredType) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public List<Map> getProcedureParameters(String name) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }
}
