package com.github.orm.elasticsearch.core.base;

import lombok.Data;

/**
 * @ClassName ESBaseEntity
 * @Description es返回包装数据
 * @Author liyongbing
 * @Date 2022/9/7 11:29
 * @Version 1.0
 **/
@Data
public class ESBaseEntity<T> {
    private Object[] sortValues;
    private float score;
    private String indexId;
    private String hitIndex;
    private T data;
}
