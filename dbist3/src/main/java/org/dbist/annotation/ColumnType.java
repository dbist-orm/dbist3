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
package org.dbist.annotation;

import org.dbist.metadata.Column;

/**
 * Defines a type or personality of a column
 *
 * @author Steve M. Jung
 * @since 2012. 2. 18. (version 1.0.0)
 */
public enum ColumnType {
    /**
     * Indicates the type that can be inferred automatically
     */
    EMPTY(""),
    /**
     *
     */
    TITLE(Column.TYPE_TITLE),
    /**
     *
     */
    LISTED(Column.TYPE_LISTED),
    /**
     *
     */
    PASSWORD(Column.TYPE_PASSWORD),
    /**
     * Indicates the Clob or Text (a long string) type of a DBMS
     */
    TEXT(Column.TYPE_TEXT),
    /**
     *
     */
    DATETIME(Column.TYPE_DATETIME);

    private final String value;

    ColumnType(String value) {
        this.value = value;
    }

    public String value() {
        return this.value;
    }
}
