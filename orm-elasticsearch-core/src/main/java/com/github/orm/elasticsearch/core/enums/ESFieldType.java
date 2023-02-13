package com.github.orm.elasticsearch.core.enums;

import com.github.orm.elasticsearch.core.base.GEOLocation;

/**
 * @author liyongbing
 * @date 2020/07/23
 */
public enum ESFieldType {
    TEXT("text"),
    BYTE("byte"),
    SHORT("short"),
    INTEGER("integer"),
    LONG("long"),
    DATE("date"),
    FLOAT("float"),
    DOUBLE("double"),
    BOOLEAN("boolean"),
    OBJECT("object"),
    NESTED("nested"),
    KEYWORD("keyword"),
    GEO_POINT("geo_point"),
    ;


    ESFieldType(String typeName) {
        this.typeName = typeName;
    }

    public String typeName;

    public static ESFieldType trans2EsType(Class<?> type) {
        ESFieldType result = null;
        if (type.equals(String.class)) {
            result = ESFieldType.KEYWORD;
        } else if (type.equals(Long.class) || type.equals(long.class)) {
            result = ESFieldType.LONG;
        } else if (type.equals(Integer.class) || type.equals(int.class)) {
            result = ESFieldType.INTEGER;
        } else if (type.equals(Byte.class) || type.equals(byte.class)) {
            result = ESFieldType.BYTE;
        } else if (type.equals(Short.class) || type.equals(short.class)) {
            result = ESFieldType.SHORT;
        } else if (type.equals(Float.class) || type.equals(float.class)) {
            result = ESFieldType.FLOAT;
        } else if (type.equals(Double.class) || type.equals(double.class)) {
            result = ESFieldType.DOUBLE;
        } else if (type.equals(Boolean.class) || type.equals(boolean.class)) {
            result = ESFieldType.BOOLEAN;
        } else if (type.equals(java.util.Date.class)) {
            result = ESFieldType.DATE;
        } else if (type.equals(GEOLocation.class)) {
            result = ESFieldType.GEO_POINT;
        }
        return result;
    }
}
