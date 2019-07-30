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
 * Is used to specify a mapped table for a class.<br>
 * If no Table annotation is specified, the default values are applied.
 *
 * <p>
 * Examples
 *
 * <pre>
 * &#064;Table(name = &quot;comments&quot;)
 * public class Comment {
 * ...
 * }
 *
 * &#064;Table(name = &quot;users&quot;)
 * public class User {
 * ...
 * }
 * </pre>
 *
 * @author Steve M. Jung
 * @since 2012. 1. 5. (version 0.0.1)
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Table {
    /**
     * Database domain
     */
    String domain() default "";

    /**
     * ddl 실행시에 무시할 지 여부
     */
    boolean ignoreDdl() default false;

    /**
     * (Optional) The name of the table.
     * <p>
     * The default value is applied to the following rules and order.<br>
     * 1. the underscore case name of the class<br>
     * 2. the name of the class
     */
    String name() default "";

    /**
     * ID Generation Strategy
     */
    String idStrategy() default GenerationRule.AUTO_INCREMENT;

    /**
     * idStrategy 값이 GenerationRule.MEANINGFUL인 경우 MEANINGFUL ID를 생성하기 위한 필드 리스트
     */
    String meaningfulFields() default "";

    /**
     * Not Null Field
     * @return
     */
    String notnullFields() default "";

    /**
     * ID Field
     */
    String idField() default "id";

    /**
     * Title Field
     */
    String titleField() default "name";

    /**
     * Unique Field
     */
    String uniqueFields() default "";

    /**
     * Version Management - version, active 필드가 존재해야 하고 마지막 버전은 반드시 active가 true 나머지는 false가 되어야 한다.
     * titleField가 있다면 domainId + titleField + version으로 unique하고 domainId + titleField로 active가 true인 것을 찾으면 된다.
     */
    boolean manageVersion() default false;

    /**
     * Tracer Entity - Tracer Entity가 있으면 히스토리 자동관리
     */
    String tracerEntity() default "";

    /**
     * Detail Entity 정의. Detail Entity가 하나 이상이면 ',' 구분자로 구분
     *
     * @return The child entities
     */
    ChildEntity[] childEntities() default {};

    /**
     * 참조 Table 여부
     *
     * @return
     */
    boolean isRef() default false;

    /**
     * (Optional) Indexes for the table. These are only used if table generation
     * is in effect. Defaults to no additional indexes.
     *
     * @return The indexes
     */
    Index[] indexes() default {};

    boolean reservedWordTolerated() default false;
}