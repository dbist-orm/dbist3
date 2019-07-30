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
package org.dbist.dml;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.dbist.annotation.Ignore;
import org.dbist.exception.DbistRuntimeException;

import net.sf.common.util.ReflectionUtils;
import net.sf.common.util.ValueUtils;

/**
 * @author Steve M. Jung
 * @since 2011. 6. 2. (version 0.0.1)
 */
public class Filters {
    public static final String OPERATOR_AND = "and";
    public static final String OPERATOR_OR = "or";
    private String operator;
    private List<Filter> filter;
    private List<Filters> filters;

    public Filters() {
        super();
    }

    public Filters(String operator) {
        super();
        this.operator = operator;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public List<Filter> getFilter() {
        return filter;
    }

    public void setFilter(List<Filter> filter) {
        this.filter = filter;
    }

    public Filters setFilter(String leftOperand, Object rightOperand) {
        return removeFilter(leftOperand).addFilter(leftOperand, rightOperand);
    }

    public Filters setFilter(String leftOperand, String operator, Object rightOperand) {
        return removeFilter(leftOperand).addFilter(leftOperand, operator, rightOperand);
    }

    public Filters setFilter(String leftOperand, String operator, Object rightOperand, boolean caseSensitive) {
        return removeFilter(leftOperand).addFilter(leftOperand, operator, rightOperand, caseSensitive);
    }

    public Filters setFilter(String leftOperand, String operator, Object rightOperand, Character escape) {
        return removeFilter(leftOperand).addFilter(leftOperand, operator, rightOperand, escape);
    }

    public Filters setFilter(String leftOperand, String operator, Object rightOperand, boolean caseSensitive, Character escape) {
        return removeFilter(leftOperand).addFilter(leftOperand, operator, rightOperand, caseSensitive, escape);
    }

    public Filters addFilter(Filter... filter) {
        if (ValueUtils.isEmpty(filter))
            return this;
        if (this.filter == null)
            this.filter = new ArrayList<Filter>();
        for (Filter f : filter)
            this.filter.add(f);
        return this;
    }

    public Filters addFilter(String leftOperand, Object rightOperand) {
        addFilterAll(new Filter(leftOperand, rightOperand));
        return this;
    }

    public Filters addFilter(String leftOperand, String operator, Object rightOperand) {
        addFilterAll(new Filter(leftOperand, operator, rightOperand));
        return this;
    }

    public Filters addFilter(String leftOperand, String operator, Object rightOperand, boolean caseSensitive) {
        addFilterAll(new Filter(leftOperand, operator, rightOperand, caseSensitive));
        return this;
    }

    public Filters addFilter(String leftOperand, String operator, Object rightOperand, Character escape) {
        addFilterAll(new Filter(leftOperand, operator, rightOperand, escape));
        return this;
    }

    public Filters addFilter(String leftOperand, String operator, Object rightOperand, boolean caseSensitive, Character escape) {
        addFilterAll(new Filter(leftOperand, operator, rightOperand, caseSensitive, escape));
        return this;
    }

    public Filters addFilterAll(Object filter) {
        return _addFilter(filter);
    }

    public Filters addFilterAll(Object filter, String... leftOperands) {
        return _addFilter(filter, leftOperands);
    }

    @SuppressWarnings("unchecked")
    private Filters _addFilter(Object filter, String... leftOperands) {
        boolean byLeftOperands = !ValueUtils.isEmpty(leftOperands);
        Set<String> fieldNameSet = byLeftOperands ? ValueUtils.toSet(leftOperands) : null;
        if (filter instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) filter;
            for (String key : map.keySet()) {
                if (byLeftOperands) {
                    if (!fieldNameSet.contains(key))
                        continue;
                    fieldNameSet.remove(key);
                }
                addFilter(key, map.get(key));
            }
        } else if (filter instanceof Filter) {
            addFilter((Filter) filter);
        } else if (filter instanceof Filter[]) {
            addFilter((Filter[]) filter);
        } else if (filter instanceof Object[]) {
            if (!ValueUtils.isEmpty(filter)) {
                Object firstItem = ((Object[]) filter)[0];
                if (!(firstItem instanceof Filter))
                    throw new DbistRuntimeException("Unsupported filter array type: Object[] of " + firstItem.getClass().getName());
                for (Object obj : (Object[]) filter)
                    addFilter((Filter) obj);
            }
        } else if (filter instanceof List) {
            if (!ValueUtils.isEmpty(filter)) {
                Object firstItem = ((List<?>) filter).get(0);
                if (!(firstItem instanceof Filter))
                    throw new DbistRuntimeException("Unsupported filter list type: List<" + firstItem.getClass().getName() + ">");
                if (this.filter == null)
                    this.filter = new ArrayList<Filter>();
                this.filter.addAll((List<Filter>) filter);
            }
        } else {
            for (Field field : ReflectionUtils.getFieldList(filter, true)) {
                Ignore ignoreAnn = field.getAnnotation(Ignore.class);
                if (ignoreAnn != null)
                    continue;

                String key = field.getName();
                if (byLeftOperands) {
                    if (!fieldNameSet.contains(key))
                        continue;
                    fieldNameSet.remove(key);
                }
                Object value;
                try {
                    value = field.get(filter);
                } catch (IllegalArgumentException e) {
                    throw e;
                } catch (IllegalAccessException e) {
                    throw new DbistRuntimeException(e);
                }
                if (!byLeftOperands && value == null)
                    continue;
                addFilter(key, value);
            }
        }
        if (byLeftOperands && fieldNameSet.size() != 0)
            throw new IllegalArgumentException("Some of condition was not found " + fieldNameSet);
        return this;
    }

    public Filters removeFilter(String... leftOperand) {
        if (this.filter == null || ValueUtils.isEmpty(leftOperand))
            return this;
        Set<String> set = ValueUtils.toSet(leftOperand);
        Set<Filter> removeSet = new HashSet<Filter>(leftOperand.length);
        for (Filter filter : this.filter) {
            if (!set.contains(filter.getLeftOperand()))
                continue;
            removeSet.add(filter);
        }
        this.filter.removeAll(removeSet);
        return this;
    }

    public List<Filters> getFilters() {
        return filters;
    }

    public void setFilters(List<Filters> filters) {
        this.filters = filters;
    }

    public Filters addFilters(Filters filters) {
        if (this.filters == null)
            this.filters = new ArrayList<Filters>();
        this.filters.add(filters);
        return this;
    }
}
