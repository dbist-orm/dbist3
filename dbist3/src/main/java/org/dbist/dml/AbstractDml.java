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
package org.dbist.dml;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.sql.DataSource;

import org.apache.commons.collections.map.ListOrderedMap;
import org.apache.commons.lang.ClassUtils;
import org.dbist.annotation.GenerationRule;
import org.dbist.exception.DataNotFoundException;
import org.dbist.exception.DbistRuntimeException;
import org.dbist.metadata.Table;
import org.dbist.processor.Preprocessor;
import org.dbist.util.UuidGenerator;
import org.dbist.util.ValueGenerator;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import net.sf.common.util.ValueUtils;

/**
 * @author Steve M. Jung
 * @since 2012. 1. 5. (version 0.0.1)
 */
public abstract class AbstractDml implements Dml, ApplicationContextAware, BeanNameAware, InitializingBean {
    private String dbType;
    private Preprocessor preprocessor;
    private String beanName;
    private ApplicationContext applicationContext;
    private DataSource dataSource;

    public String getDbType() {
        return dbType;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType == null ? null : dbType.toLowerCase();
    }

    public Table getTable(String name) {
        Class<?> clazz = getClass(name);
        return clazz == null ? null : getTable(clazz);
    }

    public Preprocessor getPreprocessor() {
        return preprocessor;
    }

    public void setPreprocessor(Preprocessor preprocessor) {
        this.preprocessor = preprocessor;
    }

    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    protected String getBeanName() {
        return beanName;
    }

    public void setBeanName(String name) {
        this.beanName = name;
    }

    public void afterPropertiesSet() throws Exception {
    }

    protected Dml getBean() {
        return this.applicationContext.getBean(this.beanName, Dml.class);
    }

    protected <T> T select(List<T> list) {
        if (ValueUtils.isEmpty(list))
            return null;
        else if (list.size() > 1)
            throw new DbistRuntimeException("Selected data size is not 1: " + list.size());
        return list.get(0);
    }

    protected Query toQuery(Table table, Object condition, String... fieldNames) throws Exception {
        if (condition instanceof Query)
            return (Query) condition;
        Query query = new Query();
        if (condition instanceof HttpServletRequest) {
            boolean byFieldName = fieldNames != null && fieldNames.length != 0;
            Set<String> fieldNameSet = byFieldName ? ValueUtils.toSet(fieldNames) : null;
            HttpServletRequest request = (HttpServletRequest) condition;
            @SuppressWarnings("unchecked")
            Map<String, String[]> paramMap = request.getParameterMap();
            for (String key : paramMap.keySet()) {
                if (byFieldName) {
                    if (!fieldNameSet.contains(key))
                        continue;
                    fieldNameSet.remove(key);
                }
                Field field = table.getField(key);
                if (field == null)
                    continue;
                String[] values = paramMap.get(key);
                if (ValueUtils.isEmpty(values))
                    continue;
                for (String value : values)
                    query.addFilter(key, value);
            }
            if (paramMap.containsKey("pageIndex"))
                query.setPageIndex(ValueUtils.toInteger(request.getParameter("pageIndex"), 0));
            if (paramMap.containsKey("pageSize"))
                query.setPageSize(ValueUtils.toInteger(request.getParameter("pageSize"), 0));
            if (paramMap.containsKey("firstResultIndex"))
                query.setFirstResultIndex(ValueUtils.toInteger(request.getParameter("firstResultIndex"), 0));
            if (paramMap.containsKey("maxResultSize"))
                query.setMaxResultSize(ValueUtils.toInteger(request.getParameter("maxResultSize"), 0));
            if (paramMap.containsKey("operator") && table.getField("operator") == null)
                query.setOperator(request.getParameter("operator"));
        } else if (condition instanceof Filters) {
            ValueUtils.populate(condition, query);
        } else if (condition instanceof Filter) {
            query.addFilter((Filter) condition);
        } else {
            query.addFilterAll(condition, fieldNames);
        }
        return query;
    }

    protected Query toPkQuery(Object obj, Object condition) throws Exception {
        Class<?> clazz;

        if (obj instanceof Class) {
            clazz = (Class<?>) obj;
        } else if (obj instanceof String) {
            clazz = getClass((String) obj);
        } else {
            clazz = obj.getClass();
        }

        if (condition instanceof Object[] && ((Object[]) condition).length == 1) {
            condition = ((Object[]) condition)[0];
        }

        Query query = new Query();

        try {
            if (condition == null || condition instanceof Query)
                return (Query) condition;

            Table table = getTable(clazz);

            if (ValueUtils.isPrimitive(condition)) {
                String[] pkFieldNames = table.getPkFieldNames();
                if (ValueUtils.isEmpty(pkFieldNames))
                    throw new DbistRuntimeException("Couln't find primary key of table " + table.getName());

                query.addFilter(table.getPkFieldNames()[0], condition);

                return query;
            } else if (condition instanceof Object[]) {
                if (ValueUtils.isEmpty(condition))
                    throw new IllegalAccessException("Requested pk condition is empty.");

                Object[] array = (Object[]) condition;
                if (ValueUtils.isPrimitive(array[0])) {
                    String[] pkFieldNames = table.getPkFieldNames();
                    if (ValueUtils.isEmpty(pkFieldNames))
                        throw new DbistRuntimeException("Couln't find primary key of table " + table.getName());

                    int i = 0;
                    int pkFieldSize = pkFieldNames.length;

                    for (Object item : array) {
                        query.addFilter(pkFieldNames[i++], item);

                        if (i == pkFieldSize)
                            break;
                    }

                    return query;
                }
            } else if (condition instanceof List) {
                if (ValueUtils.isEmpty(condition))
                    throw new IllegalAccessException("Requested pk condition is empty.");

                List<?> list = (List<Object>) condition;
                if (ValueUtils.isPrimitive(list.get(0))) {
                    String[] pkFieldNames = table.getPkFieldNames();
                    if (ValueUtils.isEmpty(pkFieldNames))
                        throw new DbistRuntimeException("Couln't find primary key of table " + table.getName());

                    int i = 0;
                    int pkFieldSize = pkFieldNames.length;
                    for (Object item : list) {
                        query.addFilter(pkFieldNames[i++], item);
                        if (i == pkFieldSize)
                            break;
                    }

                    return query;
                }
            }

            query = toQuery(table, condition, table.getPkFieldNames());
            return query;
        } finally {
            //			query.setPageIndex(0);
            //			query.setPageSize(2);
        }
    }

    @SuppressWarnings("unchecked")
    protected static <T> T newInstance(Class<T> clazz) throws InstantiationException, IllegalAccessException {
        if (clazz.equals(Map.class))
            return (T) ListOrderedMap.class.newInstance();
        return clazz.newInstance();
    }

    @SuppressWarnings("unchecked")
    public <T> T select(T data) throws Exception {
        ValueUtils.assertNotNull("data", data);
        Class<T> clazz = (Class<T>) data.getClass();
        Query query = toPkQuery(clazz, data);
        return select(selectList(clazz, query));
    }

    @SuppressWarnings("unchecked")
    public <T> T selectWithLock(T data) throws Exception {
        ValueUtils.assertNotNull("data", data);
        Class<T> clazz = (Class<T>) data.getClass();
        Query query = toPkQuery(clazz, data);
        return select(selectListWithLock(clazz, query));
    }

    public <T> T select(Class<T> clazz, Object... pkCondition) throws Exception {
        ValueUtils.assertNotNull("clazz", clazz);
        ValueUtils.assertNotEmpty("pkCondition", pkCondition);
        Query query = toPkQuery(clazz, pkCondition);
        return select(selectList(clazz, query));
    }

    public <T> T selectWithLock(Class<T> clazz, Object... pkCondition) throws Exception {
        ValueUtils.assertNotNull("clazz", clazz);
        ValueUtils.assertNotEmpty("pkCondition", pkCondition);
        Query query = toPkQuery(clazz, pkCondition);
        return select(selectListWithLock(clazz, query));
    }

    public <T> T selectByCondition(Class<T> clazz, Object condition) throws Exception {
        ValueUtils.assertNotNull("clazz", clazz);
        ValueUtils.assertNotNull("condition", condition);
        return select(selectList(clazz, condition));
    }

    public <T> T select(String tableName, Object pkCondition, Class<T> requiredType) throws Exception {
        ValueUtils.assertNotNull("tableName", tableName);
        ValueUtils.assertNotNull("pkCondition", pkCondition);
        ValueUtils.assertNotNull("requiredType", requiredType);
        Class<?> clazz = getClass(tableName);
        Query query = toPkQuery(clazz, pkCondition);
        Object obj = select(selectList(clazz, query));
        if (obj == null)
            return null;
        String[] fieldNames = getFieldNames(query);
        return ValueUtils.populate(obj, newInstance(requiredType), fieldNames);
    }

    public <T> T selectWithLock(String tableName, Object pkCondition, Class<T> requiredType) throws Exception {
        ValueUtils.assertNotNull("tableName", tableName);
        ValueUtils.assertNotNull("pkCondition", pkCondition);
        ValueUtils.assertNotNull("requiredType", requiredType);
        Class<?> clazz = getClass(tableName);
        Query query = toPkQuery(clazz, pkCondition);
        Object obj = select(selectListWithLock(clazz, query));
        if (obj == null)
            return null;
        String[] fieldNames = getFieldNames(query);
        return ValueUtils.populate(obj, newInstance(requiredType), fieldNames);
    }

    public <T> T selectByCondition(String tableName, Object condition, Class<T> requiredType) throws Exception {
        ValueUtils.assertNotNull("tableName", tableName);
        ValueUtils.assertNotNull("condition", condition);
        ValueUtils.assertNotNull("requiredType", requiredType);
        Class<?> clazz = getClass(tableName);
        Query query = toPkQuery(clazz, condition);
        Object obj = select(selectList(clazz, query));
        if (obj == null)
            return null;
        String[] fieldNames = getFieldNames(query);
        return ValueUtils.populate(obj, newInstance(requiredType), fieldNames);
    }

    public <T> T selectByConditionWithLock(String tableName, Object condition, Class<T> requiredType) throws Exception {
        ValueUtils.assertNotNull("tableName", tableName);
        ValueUtils.assertNotNull("condition", condition);
        ValueUtils.assertNotNull("requiredType", requiredType);
        Class<?> clazz = getClass(tableName);
        Query query = toPkQuery(clazz, condition);
        Object obj = select(selectListWithLock(clazz, query));
        if (obj == null)
            return null;
        String[] fieldNames = getFieldNames(query);
        return ValueUtils.populate(obj, newInstance(requiredType), fieldNames);
    }

    public <T> T selectByConditionWithLock(Class<T> clazz, Object condition) throws Exception {
        ValueUtils.assertNotNull("clazz", clazz);
        ValueUtils.assertNotNull("condition", condition);
        return select(selectListWithLock(clazz, condition));
    }

    public <T> T selectByQl(String ql, Map<String, ?> paramMap, Class<T> requiredType) throws Exception {
        //		return select(selectListByQl(ql, paramMap, requiredType, 0, 2));
        return select(selectListByQl(ql, paramMap, requiredType, 0, 0));
    }

    public <T> T selectByQlPath(String qlPath, Map<String, ?> paramMap, Class<T> requiredType) throws Exception {
        return select(selectListByQlPath(qlPath, paramMap, requiredType, 0, 0));
    }

    public <T> T selectBySqlPath(String sqlPath, Map<String, ?> paramMap, Class<T> requiredType) throws Exception {
        return select(selectListBySqlPath(sqlPath, paramMap, requiredType, 0, 0));
    }

    public <T> Page<T> selectPage(Class<T> clazz, Query query) throws Exception {
        ValueUtils.assertNotNull("clazz", clazz);
        if (query == null)
            query = new Query();
        Page<T> page = new Page<T>();
        page.setIndex(query.getPageIndex());
        page.setSize(query.getPageSize());
        page.setFirstResultIndex(query.getFirstResultIndex());
        page.setMaxResultSize(query.getMaxResultSize());
        page.setTotalSize(selectSize(clazz, query));
        if (page.getIndex() >= 0 && page.getSize() > 0 && page.getTotalSize() > 0)
            page.setLastIndex((page.getTotalSize() / page.getSize()) - (page.getTotalSize() % page.getSize() == 0 ? 1 : 0));
        page.setList(selectList(clazz, query));
        return page;
    }

    public <T> int selectSize(String tableName, Object condition) throws Exception {
        ValueUtils.assertNotNull("tableName", tableName);
        return selectSize(getClass(tableName), condition);
    }

    public <T> List<T> selectList(String tableName, Object condition, Class<T> requiredType) throws Exception {
        ValueUtils.assertNotNull("tableName", tableName);
        ValueUtils.assertNotNull("requiredType", requiredType);
        List<?> objList = selectList(getClass(tableName), condition);
        List<T> list = new ArrayList<T>();
        String[] fieldNames = getFieldNames(condition);
        for (Object obj : objList)
            list.add(ValueUtils.populate(obj, newInstance(requiredType), fieldNames));
        return list;
    }

    public <T> List<T> selectListWithLock(String tableName, Object condition, Class<T> requiredType) throws Exception {
        ValueUtils.assertNotNull("tableName", tableName);
        ValueUtils.assertNotNull("requiredType", requiredType);
        List<?> objList = selectListWithLock(getClass(tableName), condition);
        List<T> list = new ArrayList<T>();
        String[] fieldNames = getFieldNames(condition);
        for (Object obj : objList)
            list.add(ValueUtils.populate(obj, newInstance(requiredType), fieldNames));
        return list;
    }

    private String[] getFieldNames(Object condition) {
        if (!(condition instanceof Query))
            return new String[0];
        Query query = (Query) condition;
        return ValueUtils.isEmpty(query.getSelect()) ? new String[0] : query.getSelect().toArray(new String[query.getSelect().size()]);
    }

    public <T> Page<T> selectPage(String tableName, Query query, Class<T> requiredType) throws Exception {
        ValueUtils.assertNotNull("tableName", tableName);
        ValueUtils.assertNotNull("requiredType", requiredType);
        Page<T> page = new Page<T>();
        page.setIndex(query.getPageIndex());
        page.setSize(query.getPageSize());
        page.setFirstResultIndex(query.getFirstResultIndex());
        page.setMaxResultSize(query.getMaxResultSize());
        page.setTotalSize(selectSize(tableName, query));
        if (page.getIndex() >= 0 && page.getSize() > 0 && page.getTotalSize() > 0)
            page.setLastIndex((page.getTotalSize() / page.getSize()) - (page.getTotalSize() % page.getSize() == 0 ? 1 : 0));
        page.setList(selectList(tableName, query, requiredType));
        return page;
    }

    public <T> List<T> selectListByQl(String ql, Map<String, ?> paramMap, Class<T> requiredType, int pageIndex, int pageSize) throws Exception {
        return selectListByQl(ql, paramMap, requiredType, pageIndex, pageSize, 0, 0);
    }

    public <T> Page<T> selectPageByQl(String ql, Map<String, ?> paramMap, Class<T> requiredType, int pageIndex, int pageSize) throws Exception {
        return selectPageByQl(ql, paramMap, requiredType, pageIndex, pageSize, 0, 0);
    }

    public <T> List<T> selectListByQlPath(String qlPath, Map<String, ?> paramMap, Class<T> requiredType, int pageIndex, int pageSize)
        throws Exception {
        return selectListByQlPath(qlPath, paramMap, requiredType, pageIndex, pageSize, 0, 0);
    }

    public <T> Page<T> selectPageByQlPath(String qlPath, Map<String, ?> paramMap, Class<T> requiredType, int pageIndex, int pageSize)
        throws Exception {
        return selectPageByQlPath(qlPath, paramMap, requiredType, pageIndex, pageSize, 0, 0);
    }

    public <T> T selectBySql(String sql, Map<String, ?> paramMap, Class<T> requiredType) throws Exception {
        return selectByQl(sql, paramMap, requiredType);
    }

    public <T> List<T> selectListBySql(String sql, Map<String, ?> paramMap, Class<T> requiredType, int pageIndex, int pageSize) throws Exception {
        return selectListByQl(sql, paramMap, requiredType, pageIndex, pageSize, 0, 0);
    }

    public <T> List<T> selectListBySql(String sql, Map<String, ?> paramMap, Class<T> requiredType, int pageIndex, int pageSize, int firstResultIndex,
                                       int maxResultSize) throws Exception {
        return selectListByQl(sql, paramMap, requiredType, pageIndex, pageSize, firstResultIndex, maxResultSize);
    }

    public <T> Page<T> selectPageBySql(String sql, Map<String, ?> paramMap, Class<T> requiredType, int pageIndex, int pageSize) throws Exception {
        return selectPageByQl(sql, paramMap, requiredType, pageIndex, pageSize, 0, 0);
    }

    public <T> Page<T> selectPageByQl(String ql, Map<String, ?> paramMap, Class<T> requiredType, int pageIndex, int pageSize, int firstResultIndex,
                                      int maxResultSize) throws Exception {
        paramMap = paramMap == null ? new HashMap<String, Object>() : paramMap;
        Page<T> page = new Page<T>();
        page.setIndex(pageIndex);
        page.setSize(pageSize);
        page.setFirstResultIndex(firstResultIndex);
        page.setMaxResultSize(maxResultSize);
        page.setList(selectListByQl(ql, paramMap, requiredType, pageIndex, pageSize, firstResultIndex, maxResultSize));
        page.setTotalSize(selectSizeByQl(ql, paramMap));
        if (page.getIndex() >= 0 && page.getSize() > 0 && page.getTotalSize() > 0)
            page.setLastIndex((page.getTotalSize() / page.getSize()) - (page.getTotalSize() % page.getSize() == 0 ? 1 : 0));
        return page;
    }

    public int selectSizeBySql(String sql, Map<String, ?> paramMap) throws Exception {
        return selectSizeByQl(sql, paramMap);
    }

    public <T> Page<T> selectPageBySql(String sql, Map<String, ?> paramMap, Class<T> requiredType, int pageIndex, int pageSize, int firstResultIndex,
                                       int maxResultSize) throws Exception {
        return selectPageByQl(sql, paramMap, requiredType, pageIndex, pageSize, firstResultIndex, maxResultSize);
    }

    public <T> List<T> selectListBySqlPath(String sqlPath, Map<String, ?> paramMap, Class<T> requiredType, int pageIndex, int pageSize)
        throws Exception {
        return selectListByQlPath(sqlPath, paramMap, requiredType, pageIndex, pageSize, 0, 0);
    }

    public <T> List<T> selectListBySqlPath(String sqlPath, Map<String, ?> paramMap, Class<T> requiredType, int pageIndex, int pageSize,
                                           int firstResultIndex, int maxResultSize) throws Exception {
        return selectListByQlPath(sqlPath, paramMap, requiredType, pageIndex, pageSize, firstResultIndex, maxResultSize);
    }

    public <T> Page<T> selectPageBySqlPath(String sqlPath, Map<String, ?> paramMap, Class<T> requiredType, int pageIndex, int pageSize)
        throws Exception {
        return selectPageByQlPath(sqlPath, paramMap, requiredType, pageIndex, pageSize, 0, 0);
    }

    public <T> Page<T> selectPageBySqlPath(String sqlPath, Map<String, ?> paramMap, Class<T> requiredType, int pageIndex, int pageSize,
                                           int firstResultIndex, int maxResultSize) throws Exception {
        return selectPageByQlPath(sqlPath, paramMap, requiredType, pageIndex, pageSize, firstResultIndex, maxResultSize);
    }

    public int selectSizeBySqlPath(String sqlPath, Map<String, ?> paramMap) throws Exception {
        return selectSizeByQlPath(sqlPath, paramMap);
    }

    public <T> T insert(Class<T> clazz, Object data) throws Exception {
        return _insert(clazz, data);
    }

    public void insertBatch(Class<?> clazz, List<?> list) throws Exception {
        insertBatch(toRequiredType(list, clazz));
    }

    public void insert(Class<?> clazz, Object data, String... fieldNames) throws Exception {
        _insert(clazz, data, fieldNames);
    }

    public void insertBatch(Class<?> clazz, List<?> list, String... fieldNames) throws Exception {
        insertBatch(toRequiredType(list, clazz), fieldNames);
    }

    @SuppressWarnings("unchecked")
    private <T> T _insert(Class<T> clazz, Object data, String... fieldNames) throws Exception {
        ValueUtils.assertNotNull("clazz", clazz);
        ValueUtils.assertNotNull("data", data);
        if (data.getClass().isAssignableFrom(clazz)) {
            T obj = (T) data;
            insert(obj);
            return obj;
        }
        T obj = ValueUtils.populate(data, clazz.newInstance());
        getBean().insert(obj, fieldNames);
        return obj;
    }

    public void insert(String tableName, Object data) throws Exception {
        insert(getClass(tableName), data);
    }

    public void insertBatch(String tableName, List<?> list) throws Exception {
        insertBatch(getClass(tableName), list);
    }

    public void insert(String tableName, Object data, String... fieldNames) throws Exception {
        insert(getClass(tableName), data, fieldNames);
    }

    public void insertBatch(String tableName, List<?> list, String... fieldNames) throws Exception {
        insertBatch(getClass(tableName), list, fieldNames);
    }

    protected ValueGenerator getValueGenerator(String generator) {
        if (ValueUtils.isEmpty(generator))
            return null;
        if (GenerationRule.UUID.equals(generator)) {
            try {
                return UuidGenerator.class.newInstance();
            } catch (InstantiationException e) {
                throw new DbistRuntimeException(e);
            } catch (IllegalAccessException e) {
                throw new DbistRuntimeException(e);
            }
        }
        try {
            return (ValueGenerator) ClassUtils.getClass(generator).newInstance();
        } catch (InstantiationException e) {
            throw new DbistRuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new DbistRuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new DbistRuntimeException(e);
        }
    }

    protected <T> void doBeforeInsert(T data, Table table) throws Exception {
        for (Field field : table.getValueGeneratorByFieldMap().keySet()) {
            if (!ValueUtils.isEmpty(field.get(data)))
                continue;
            ValueGenerator gen = table.getValueGeneratorByFieldMap().get(field);
            gen.generate(data, table.getColumnByFieldName(field.getName()));
        }
    }

    protected <T> void doBeforeInsertBatch(List<T> list, Table table) throws Exception {
        if (ValueUtils.isEmpty(list) || table.getValueGeneratorByFieldMap().isEmpty())
            return;
        for (T data : list)
            doBeforeInsert(data, table);
    }

    public <T> T update(Class<T> clazz, Object data) throws Exception {
        return _update(clazz, data);
    }

    public void updateBatch(Class<?> clazz, List<?> list) throws Exception {
        updateBatch(toRequiredType(list, clazz));
    }

    public <T> T update(Class<T> clazz, Object data, String... fieldNames) throws Exception {
        return _update(clazz, data, fieldNames);
    }

    public void updateBatch(Class<?> clazz, List<?> list, String... fieldNames) throws Exception {
        updateBatch(toRequiredType(list, clazz, fieldNames), fieldNames);
    }

    private <T> T _update(Class<T> clazz, Object data, String... fieldNames) throws Exception {
        ValueUtils.assertNotNull("clazz", clazz);
        ValueUtils.assertNotNull("data", data);
        if (data.getClass().equals(clazz)) {
            @SuppressWarnings("unchecked")
            T obj = (T) data;
            update(obj, fieldNames);
            return obj;
        }
        T obj = select(clazz, data);
        if (obj == null) {
            Table table = getTable(data);
            throw new DataNotFoundException("Couldn't find data from table[" + table.getDomain() + "." + table.getName() + "]");
        }
        obj = ValueUtils.populate(data, obj, fieldNames);
        getBean().update(obj, fieldNames);
        return obj;
    }

    @SuppressWarnings("unchecked")
    private static <T> List<T> toRequiredType(List<?> list, Class<T> requiredType, String... fieldNames) throws InstantiationException,
        IllegalAccessException {
        if (ValueUtils.isEmpty(list))
            return (List<T>) list;
        if (list.get(0).getClass().equals(requiredType))
            return (List<T>) list;
        List<T> dataList = new ArrayList<T>(list.size());
        for (Object obj : list)
            dataList.add(ValueUtils.populate(obj, newInstance(requiredType), fieldNames));
        return dataList;
    }

    public void update(String tableName, Object data) throws Exception {
        update(getClass(tableName), data);
    }

    public void updateBatch(String tableName, List<?> list) throws Exception {
        updateBatch(getClass(tableName), list);
    }

    public void update(String tableName, Object data, String... fieldNames) throws Exception {
        update(getClass(tableName), data, fieldNames);
    }

    public void updateBatch(String tableName, List<?> list, String... fieldNames) throws Exception {
        updateBatch(getClass(tableName), list, fieldNames);
    }

    public void upsert(Object data) throws Exception {
        _upsert(data);
    }

    public void upsertBatch(List<?> list) throws Exception {
        _upsertBatch(list);
    }

    public void upsert(Object data, String... fieldNames) throws Exception {
        _upsert(data, fieldNames);
    }

    public void upsertBatch(List<?> list, String... fieldNames) throws Exception {
        _upsertBatch(list, fieldNames);
    }

    public void _upsert(Object data, String... fieldNames) throws Exception {
        if (data == null)
            return;
        Query query = toPkQuery(data.getClass(), data);
        if (selectSize(data.getClass(), query) == 0)
            insert(data, fieldNames);
        else
            update(data, fieldNames);
    }

    public void _upsertBatch(List<?> list, String... fieldNames) throws Exception {
        List<Object> insertList = new ArrayList<Object>();
        List<Object> updateList = new ArrayList<Object>();
        for (Object data : list) {
            if (select(data) == null)
                insertList.add(data);
            else
                updateList.add(data);
        }
        insertBatch(insertList, fieldNames);
        updateBatch(updateList, fieldNames);
    }

    public <T> T upsert(Class<T> clazz, Object data) throws Exception {
        return select(clazz, data) == null ? insert(clazz, data) : update(clazz, data);
    }

    public void upsertBatch(Class<?> clazz, List<?> list) throws Exception {
        List<?> newList = toRequiredType(list, clazz);
        upsertBatch(newList);
    }

    public void upsert(Class<?> clazz, Object data, String... fieldNames) throws Exception {
        if (select(clazz, data) == null)
            insert(clazz, data, fieldNames);
        else
            update(clazz, data, fieldNames);
    }

    public void upsertBatch(Class<?> clazz, List<?> list, String... fieldNames) throws Exception {
        List<?> newList = toRequiredType(list, clazz, fieldNames);
        upsertBatch(newList, fieldNames);
    }

    public void upsert(String tableName, Object data) throws Exception {
        upsert(getClass(tableName), data);
    }

    public void upsertBatch(String tableName, List<?> list) throws Exception {
        upsertBatch(getClass(tableName), list);
    }

    public void upsert(String tableName, Object data, String... fieldNames) throws Exception {
        upsert(getClass(tableName), data, fieldNames);
    }

    public void upsertBatch(String tableName, List<?> list, String... fieldNames) throws Exception {
        upsertBatch(getClass(tableName), list, fieldNames);
    }

    public <T> T delete(Class<T> clazz, Object... pkCondition) throws Exception {
        T data = select(clazz, pkCondition);
        if (data == null)
            return null;
        delete(data);
        return data;
    }

    public void deleteBatch(Class<?> clazz, List<?> list) throws Exception {
        deleteBatch(toRequiredType(list, clazz));
    }

    public <T> T deleteByCondition(Class<T> clazz, Object condition) throws Exception {
        T data = selectByCondition(clazz, condition);
        if (data == null)
            return null;
        delete(data);
        return data;
    }

    public void delete(String tableName, Object... pkCondition) throws Exception {
        delete(getClass(tableName), pkCondition);
    }

    public void deleteBatch(String tableName, List<?> list) throws Exception {
        deleteBatch(getClass(tableName), list);
    }

    public void deleteByCondition(String tableName, Object condition) throws Exception {
        Object data = selectByCondition(tableName, condition, getClass(tableName));
        if (data == null)
            return;
        delete(data);
    }

    public int executeBySql(String sql, Map<String, ?> paramMap) throws Exception {
        return executeByQl(sql, paramMap);
    }

    public int executeBySqlPath(String sqlPath, Map<String, ?> paramMap) throws Exception {
        return executeByQlPath(sqlPath, paramMap);
    }
}
