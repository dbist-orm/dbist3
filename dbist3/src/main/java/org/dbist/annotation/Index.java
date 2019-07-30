package org.dbist.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * The Index annotation is used in schema generation. Note that it is not necessary to specify an index
 * for a primary key, as the primary key index will be created automatically, however, the Index annotation
 * may be used to specify the ordering of the columns in the index for the primary key.
 */
@Target({})
@Retention(RUNTIME)
public @interface Index {
    /**
     * (Optional) The name of the index.  Defaults to a provider-generated value.
     *
     * @return The index name
     */
    String name() default "";

    /**
     * (Required) The names of the columns to be included in the index.
     *
     * @return The names of the columns making up the index
     */
    String columnList();

    /**
     * (Optional) Whether the index is unique.  Default is false.
     *
     * @return Is the index unique?
     */
    boolean unique() default false;
}