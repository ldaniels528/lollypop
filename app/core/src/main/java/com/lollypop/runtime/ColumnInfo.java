package com.lollypop.runtime;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Column Information
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.PARAMETER, ElementType.CONSTRUCTOR, ElementType.FIELD})
public @interface ColumnInfo {

    /**
     * The optional column type definition (e.g. "SEQUENCE_NUMBER(1)")
     * @return the optional column type definition
     */
    String typeDef() default "";

    /**
     * Indicates whether the contents of the column can be null (defaults to `true`)
     * @return the nullable indicator
     */
    boolean isNullable() default true;

    /**
     * Indicates whether the contents of the column should be compressed (defaults to `false`)
     * @return the compression indicator
     */
    boolean isCompressed() default false;

    /**
     * Sets the maximum column size (defaults to `0`)
     * @return the maximum column size
     */
    int maxSize() default 0;

}
