package com.github.orm.elasticsearch.core.base;

import com.github.orm.elasticsearch.core.annotation.ESField;
import com.github.orm.elasticsearch.core.enums.ESFieldType;
import lombok.Data;
import lombok.SneakyThrows;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @ClassName ReflectionUtils
 * @Description
 * @Author liyongbing
 * @Date 2022/8/1 14:40
 * @Version 1.0
 **/
public class ReflectionUtils {

    private static ConcurrentHashMap<Class, Map<String, Field>> fieldCache = new ConcurrentHashMap<>();


    public static Map<String, Field> findFields(Class<?> clazz) {
        Map<String, Field> fieldMap = fieldCache.get(clazz);
        if (fieldMap == null) {
            synchronized (ReflectionUtils.class) {
                fieldMap = fieldCache.get(clazz);
                if (fieldMap == null) {
                    fieldMap = new HashMap<>();
                    fieldCache.put(clazz, fieldMap);
                    for (; !clazz.equals(Object.class); clazz = clazz.getSuperclass()) {
                        Field[] fields = clazz.getDeclaredFields();
                        for (Field field : fields) {
                            fieldMap.putIfAbsent(field.getName(), field);
                        }
                    }
                }
            }
        }
        return fieldCache.get(clazz);
    }

    public static Field findField(Class<?> clazz, String fieldName) {
        return findFields(clazz).get(fieldName);
    }


    @SneakyThrows
    public static Object getFieldValue(Object obj, String fieldName) {
        Field field = findField(obj.getClass(), fieldName);
        if (!field.isAccessible()) {
            field.setAccessible(true);
        }
        return field.get(obj);
    }

    @SneakyThrows
    public static void setFieldValue(Object obj, String fieldName, Object value) {
        Field field = findField(obj.getClass(), fieldName);
        if (!field.isAccessible()) {
            field.setAccessible(true);
        }
        field.set(obj, value);
    }


    /**
     * 获取field类型或集合类型内的真实类型
     *
     * @param field
     * @return
     */
    public static Class getTypeOrCollectionRealType(Field field) {
        Class<?> type = field.getType();
        if (Collection.class.isAssignableFrom(type)) {
            Type genericType = field.getGenericType();
            if (genericType instanceof ParameterizedType) {
                Type typeArgument = ((ParameterizedType) genericType).getActualTypeArguments()[0];
                if (typeArgument instanceof Class) {
                    type = (Class<?>) typeArgument;
                }
            }
        }
        return type;
    }

    /**
     * 获取 ESField 信息
     *
     * @param field
     * @return
     */
    public static ESFieldData getESFieldData(Field field) {
        ESFieldData data = new ESFieldData();
        data.setName(field.getName());
        ESField annotation = field.getAnnotation(ESField.class);
        if (annotation != null) {
            data.setFieldType(annotation.type());
            data.setAnalyzer(annotation.analyzer());
            data.setTextRaw(annotation.textRaw());
            data.setTextRawName(annotation.textRawName());
            data.setTextRawIgnoreAbove(annotation.textRawIgnoreAbove());
        } else {
            data.setFieldType(ESFieldType.trans2EsType(field.getType()));
        }
        return data;
    }


    @Data
    public static class ESFieldData {
        private String name;
        private String analyzer;
        private ESFieldType fieldType;
        private boolean textRaw;
        private String textRawName;
        private int textRawIgnoreAbove;
    }

}
