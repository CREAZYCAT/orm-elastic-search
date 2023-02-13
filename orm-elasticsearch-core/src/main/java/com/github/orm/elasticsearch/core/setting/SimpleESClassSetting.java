package com.github.orm.elasticsearch.core.setting;

import com.github.orm.elasticsearch.core.annotation.ESDocument;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.util.Objects;

/**
 * @ClassName SimpleESClassSetting
 * @Description
 * @Author liyongbing
 * @Date 2022/9/13 10:22
 * @Version 1.0
 **/
public class SimpleESClassSetting implements ESClassSetting {
    protected int shards;
    protected int replicas;
    protected XContentBuilder builder;

    @Override
    public ESClassSetting setDocType(Class<?> type) throws Exception {
        ESDocument esDocument = Objects.requireNonNull(type.getDeclaredAnnotation(ESDocument.class));
        this.shards = esDocument.shards() > 0 ? esDocument.shards() : 3;
        this.replicas = esDocument.replicas();
        this.builder = XContentFactory.jsonBuilder();
        return this;
    }

    @Override
    public ESClassSetting setting() throws Exception {
        builder.startObject();
        builder.startObject("index");
        builder.field("number_of_shards", shards);
        builder.field("number_of_replicas", replicas);
        builder.endObject();
        builder.endObject();
        return this;
    }

    @Override
    public XContentBuilder getBuilder() throws Exception {
        return builder;
    }
}
