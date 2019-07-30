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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Is used to specify a mapped column for a field.<br>
 * If no Column annotation is specified, the default values are applied.
 *
 * <p>
 * Examples:
 *
 * <pre>
 * &#064;Column(name = &quot;pwd&quot;, type = ColumnType.PASSWORD)
 * private String password;
 *
 * &#064;Column(type = ColumnType.TITLE)
 * private String title;
 *
 * &#064;Column(type = ColumnType.LISTED)
 * private String author;
 *
 * &#064;Column(type = ColumnType.TEXT)
 * private String content;
 * </pre>
 *
 * @author Steve M. Jung
 * @since 2011. 7. 10. (version 0.0.1)
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Column {
    /**
     * (Optional) The name of the column.
     * <p>
     * The default value is applied to the following rules and order.<br>
     * 1. the underscore case name of the field<br>
     * 2. the name of the field
     */
    String name() default "";

    /**
     * (Optional) Whether the column is a unique key. This is a shortcut for the
     * <code>UniqueConstraint</code> annotation at the table level and is useful
     * for when the unique key constraint corresponds to only a single column.
     * This constraint applies in addition to any constraint entailed by primary
     * key mapping and to constraints specified at the table level.
     */
    boolean unique() default false;

    /**
     * (Optional) Whether the database column is nullable.
     */
    boolean nullable() default true;

    /**
     * (Optional) The column length. (Applies only if a string-valued column is
     * used.)
     */
    int length() default 255;

    /**
     * (Optional) The type or personality of the column<br>
     *
     * @see ColumnType
     */
    ColumnType type() default ColumnType.EMPTY;

    /**
     * (Optional) A generation rule name or a generation class name
     *
     * @return
     */
    String generator() default GenerationRule.NONE;
}
