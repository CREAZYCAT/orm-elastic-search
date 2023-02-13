package com.github.orm.elasticsearch.core.base;

import com.alibaba.fastjson.JSON;
import com.github.orm.elasticsearch.core.setting.ESClassSetting;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.HttpAsyncResponseConsumerFactory;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

/**
 * @author liyongbing
 * @date 2020/07/23
 */
@Slf4j
public class ElasticsearchUtils {
    private RestHighLevelClient client;

    public static final RequestOptions COMMON_OPTIONS;

    static {
        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();

        // 默认缓冲限制为100MB，此处修改为30MB。
        builder.setHttpAsyncResponseConsumerFactory(new HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory(30 * 1024 * 1024));
        COMMON_OPTIONS = builder.build();
    }

    public RestHighLevelClient getClient() {
        return client;
    }

    public void setClient(RestHighLevelClient client) {
        this.client = client;
    }

    public boolean existIndex(String indexName) {
        boolean exists = false;
        try {
            GetIndexRequest request = new GetIndexRequest(indexName);
            exists = client.indices().exists(request, RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new ElasticsearchException("判断索引 {" + indexName + "} 是否存在失败", e);
        }
        return exists;
    }

    public void createIndexRequest(String indexName, Class clazz, ESClassSetting setting) {
        if (existIndex(indexName)) {
            log.info("index exist:[{}]", indexName);
            return;
        }

        try {
            CreateIndexRequest request = new CreateIndexRequest(indexName);
            // Settings for this index
//            request.settings(Settings.builder()
//                    .put("index.number_of_shards", shards)
//                    .put("index.number_of_replicas", replicas)
//            );
            request.settings(setting.setDocType(clazz).setting().getBuilder());
            request.mapping(new ESClassLoopMapping(clazz).mapping().getBuilder());
            CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
            log.info(" acknowledged : {}", createIndexResponse.isAcknowledged());
            log.info(" shardsAcknowledged :{}", createIndexResponse.isShardsAcknowledged());
        } catch (Exception e) {
            throw new ElasticsearchException("创建索引 {" + indexName + "} 失败", e);
        }
    }


    public void deleteIndexRequest(String index) {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(index);
        try {
            client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new ElasticsearchException("删除索引 {" + index + "} 失败");
        }
    }

    public static IndexRequest buildIndexRequest(String index, String id, Object object) {
        return new IndexRequest(index).id(id).source(JSON.toJSONString(object), XContentType.JSON);
    }

    public void updateRequest(String index, String id, Object object) {
        try {
            UpdateRequest updateRequest = new UpdateRequest(index, id).doc(JSON.toJSONString(object), XContentType.JSON);
            client.update(updateRequest, COMMON_OPTIONS);
        } catch (IOException e) {
            throw new ElasticsearchException("更新索引 {" + index + "} 数据 {" + object + "} 失败");
        }
    }

    public void deleteRequest(String index, String id) {
        try {
            DeleteRequest deleteRequest = new DeleteRequest(index, id);
            client.delete(deleteRequest, COMMON_OPTIONS);
        } catch (IOException e) {
            throw new ElasticsearchException("删除索引 {" + index + "} 数据id {" + id + "} 失败");
        }
    }
}
