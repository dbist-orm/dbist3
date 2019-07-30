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

import java.util.ArrayList;
import java.util.List;

import net.sf.common.util.ValueUtils;

/**
 * @author Steve M. Jung
 * @since 2011. 6. 2. (version 0.0.1)
 */
public class Filter {

    private String leftOperand;
    private String operator;
    private List<?> rightOperand;
    private boolean caseSensitive = true;
    private Character escape;

    private String name;
    private Object value;

    public Filter(String leftOperand, Object rightOperand) {
        this(leftOperand, "=", rightOperand);
    }

    public Filter(String leftOperand, String operator, Object rightOperand) {
        this.setLeftOperand(leftOperand);
        this.setOperator(operator);
        if (rightOperand != null)
            addRightOperand(rightOperand);
    }

    public Filter(String leftOperand, String operator, Object rightOperand, boolean caseSensitive) {
        this.setLeftOperand(leftOperand);
        this.setOperator(operator);
        this.addRightOperand(rightOperand);
        this.setCaseSensitive(caseSensitive);
    }

    public Filter(String leftOperand, String operator, Object rightOperand, Character escape) {
        this.setLeftOperand(leftOperand);
        this.setOperator(operator);
        if (rightOperand != null)
            this.addRightOperand(rightOperand);
        this.setCaseSensitive(caseSensitive);
    }

    public Filter(String leftOperand, String operator, Object rightOperand, boolean caseSensitive, Character escape) {
        this.setLeftOperand(leftOperand);
        this.setOperator(operator);
        if (rightOperand != null)
            this.addRightOperand(rightOperand);
        this.setCaseSensitive(caseSensitive);
        this.setEscape(escape);
    }

    public String getLeftOperand() {
        if (leftOperand == null) {
            this.setLeftOperand(this.name);
        }
        return leftOperand;
    }

    public void setLeftOperand(String leftOperand) {
        this.leftOperand = leftOperand;
    }

    public String getOperator() {
        return this.getOperator(true);
    }

    public String getOperator(boolean parseOperator) {
        if (parseOperator)
            this.normalizeOperator();
        return this.operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
        this.normalizeOperator();
        this.normalizeValues();
    }

    public List<?> getRightOperand() {
        if (rightOperand == null) {
            this.normalizeValues();
        }

        return rightOperand;
    }

    public void setRightOperand(List<?> rightOperand) {
        this.rightOperand = rightOperand;
    }

    public void setRightOperand(Object... rightOperand) {
        this.rightOperand = ValueUtils.toList(rightOperand);
    }

    public Filter addRightOperand(Object... rightOperand) {
        if (ValueUtils.isEmpty(rightOperand))
            return this;
        if (this.rightOperand == null)
            this.rightOperand = new ArrayList<Object>();
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) this.rightOperand;
        for (Object ro : rightOperand) {
            if (ro == null) {
                list.add(null);
            } else if (ro instanceof Object[]) {
                for (Object subRo : (Object[]) ro)
                    list.add(subRo);
            } else if (ro instanceof List) {
                for (Object subRo : (List<?>) ro)
                    list.add(subRo);
            } else {
                list.add(ro);
            }
        }
        return this;
    }

    public boolean isCaseSensitive() {
        return caseSensitive;
    }

    public void setCaseSensitive(boolean caseSensitive) {
        this.caseSensitive = caseSensitive;
    }

    public Character getEscape() {
        return escape;
    }

    public void setEscape(Character escape) {
        this.escape = escape;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
        this.leftOperand = name;
    }

    public void setValue(Object value) {
        this.value = value;
        this.normalizeValues();
    }

    public Object getValue() {
        return this.value;
    }

    private void normalizeOperator() {
        if (this.operator == null || this.operator.equalsIgnoreCase("eq")) {
            this.operator = "=";
        } else if (this.operator.equalsIgnoreCase("noteq")) {
            this.operator = "<>";
        } else if (this.operator.equalsIgnoreCase("notin")) {
            this.operator = "not in";
        } else if (this.operator.equalsIgnoreCase("is_null")) {
            this.operator = "is null";
        } else if (this.operator.equalsIgnoreCase("is_not_null")) {
            this.operator = "is not null";
        } else if (this.operator.equalsIgnoreCase("is_false")) {
            this.operator = "is false";
        } else if (this.operator.equalsIgnoreCase("is_true")) {
            this.operator = "is true";
        } else if (this.operator.equalsIgnoreCase("is_not_false")) {
            this.operator = "is not false";
        } else if (this.operator.equalsIgnoreCase("is_not_true")) {
            this.operator = "is not true";
        } else if (this.operator.equalsIgnoreCase("is_present")) {
            this.operator = "is present";
        } else if (this.operator.equalsIgnoreCase("is_blank")) {
            this.operator = "is blank";
        } else if (this.operator.equalsIgnoreCase("is_empty_num_id")) {
            this.operator = "is empty numeric id";
        } else if (this.operator.equalsIgnoreCase("gt")) {
            this.operator = ">";
        } else if (this.operator.equalsIgnoreCase("gte")) {
            this.operator = ">=";
        } else if (this.operator.equalsIgnoreCase("lt")) {
            this.operator = "<";
        } else if (this.operator.equalsIgnoreCase("lte")) {
            this.operator = "<=";
        } else if (this.operator.equalsIgnoreCase("nlike")) {
            this.operator = "not like";
        } else if (this.operator.equalsIgnoreCase("between")) {
            if (this.value != null && this.value instanceof String) {
                String valueStr = (String) this.value;
                if (valueStr.charAt(0) == ',') {
                    this.operator = "<=";
                    this.value = valueStr.replace(",", "");
                } else if (valueStr.charAt(valueStr.length() - 1) == ',') {
                    this.operator = ">=";
                    this.value = valueStr.replace(",", "");
                }
            }
        }
    }

    private void normalizeValues() {
        if (this.operator != null && this.value != null) {
            if (this.operator.equalsIgnoreCase("in") || this.operator.equalsIgnoreCase("not in") || this.operator.equalsIgnoreCase("between")) {
                if (value instanceof String) {
                    String[] values = value.toString().split(",");
                    for (int i = 0; i < values.length; i++) {
                        this.addRightOperand(values[i]);
                    }
                } else {
                    this.setRightOperand(this.value);
                }
            } else {
                this.setRightOperand(this.value);
            }
        }
    }

}
