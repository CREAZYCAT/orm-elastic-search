package com.github.orm.elasticsearch.core.annotation;

import com.github.orm.elasticsearch.core.enums.ESFieldType;

import java.lang.annotation.*;

/**
 * @author liyongbing
 * @date 2020/07/23
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@Documented
@Inherited
public @interface ESField {

    String value() default "";

    String name() default "";

    ESFieldType type() default ESFieldType.KEYWORD;

    String analyzer() default "";

    boolean textRaw() default false;

    String textRawName() default "raw";

    int textRawIgnoreAbove() default 256;
}
