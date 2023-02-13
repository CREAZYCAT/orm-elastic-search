package com.github.orm.elasticsearch.core.base;

import com.alibaba.fastjson.JSON;
import com.github.orm.elasticsearch.core.annotation.ESDocument;
import com.github.orm.elasticsearch.core.annotation.ESId;
import com.github.orm.elasticsearch.core.setting.ESClassSetting;
import com.github.orm.elasticsearch.core.setting.SimpleESClassSetting;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.GenericTypeResolver;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;

/**
 * @author liyongbing
 * @date 2020/07/23
 */
@Slf4j
public abstract class BaseElasticsearchDao<T> implements InitializingBean {

    protected ElasticsearchUtils elasticsearchUtils;
    protected String env;

    /**
     * 索引名称
     */
    protected String indexName;
    /**
     * ID字段
     */
    protected Field idField;
    /**
     * T对应的类型Class
     */
    protected Class<T> genericClass;

    protected int shards;

    protected int replicas;

    public ElasticsearchUtils getElasticsearchUtils() {
        return elasticsearchUtils;
    }

    public RestHighLevelClient getClient() {
        return getElasticsearchUtils().getClient();
    }

    public String getIndexName() {
        return indexName;
    }

    public String getIndexName(T data) {
        return indexName;
    }

    public String getIndexNamePattern() {
        return indexName;
    }

    public Field getIdField() {
        return idField;
    }

    public Class<T> getGenericClass() {
        return genericClass;
    }

    public BaseElasticsearchDao(ElasticsearchUtils elasticsearchUtils, String env) {
        this.elasticsearchUtils = elasticsearchUtils;
        this.env = env;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void afterPropertiesSet() throws Exception {
        Class<T> beanClass = (Class<T>) GenericTypeResolver.resolveTypeArgument(this.getClass(), BaseElasticsearchDao.class);
        this.genericClass = beanClass;
        ESDocument esDocument = AnnotationUtils.findAnnotation(Objects.requireNonNull(beanClass), ESDocument.class);
        this.indexName = Objects.requireNonNull(esDocument).indexName() + "_" + env;
        this.shards = esDocument.shards() > 0 ? esDocument.shards() : 3;
        this.replicas = esDocument.replicas();
        Field[] declaredFields = beanClass.getDeclaredFields();
        for (Field declaredField : declaredFields) {
            ESId esId = declaredField.getAnnotation(ESId.class);
            if (esId != null) {
                this.idField = declaredField;
                idField.setAccessible(true);
                break;
            }
        }
        createIndex();
    }

    public void createIndex() {
        if (this.getElasticsearchUtils().existIndex(getIndexName())) {
            return;
        }
        this.getElasticsearchUtils().createIndexRequest(getIndexName(), genericClass, classSetting());
    }

    public void refreshIndex() {
        if (this.getElasticsearchUtils().existIndex(getIndexName())) {
            this.getElasticsearchUtils().deleteIndexRequest(getIndexName());
        }
        this.getElasticsearchUtils().createIndexRequest(getIndexName(), genericClass, classSetting());
    }

    public ESClassSetting classSetting() {
        return new SimpleESClassSetting();
    }


    public void saveOrUpdate(T genericInstance) {
        IndexRequest request = ElasticsearchUtils.buildIndexRequest(getIndexName(genericInstance), getIdValue(genericInstance), genericInstance);
        try {
            getClient().index(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("elasticsearch insert error", e);
        }
    }


    public void batchSaveOrUpdate(List<T> indexList) {
        if (CollectionUtils.isEmpty(indexList)) {
            return;
        }
        BulkRequest request = new BulkRequest();
        for (T index : indexList) {
            request.add(ElasticsearchUtils.buildIndexRequest(getIndexName(index), getIdValue(index), index));
        }
        try {
            BulkResponse response = getClient().bulk(request, RequestOptions.DEFAULT);
            if (response.hasFailures()) {
                throw new RuntimeException(response.buildFailureMessage());
            }
        } catch (IOException e) {
            log.error("elasticsearch batch insert error", e);
        }
    }

    public void delete(T genericInstance) {
        if (ObjectUtils.isEmpty(genericInstance)) {
            return;
        }
        elasticsearchUtils.deleteRequest(getIndexName(genericInstance), getIdValue(genericInstance));
    }

    public void delete(List<T> indexList) {
        if (CollectionUtils.isEmpty(indexList)) {
            return;
        }
        BulkRequest request = new BulkRequest();
        for (T index : indexList) {
            request.add(new DeleteRequest(getIndexName(index), getIdValue(index)));
        }
        try {
            getClient().bulk(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("elasticsearch batch delete error", e);
        }
    }


    public List<T> search(SearchSourceBuilder searchSourceBuilder) {
        ESPageResult<T> search = search(searchSourceBuilder, null, null);
        return search != null ? search.getResults() : null;
    }


    public ESPageResult<T> search(SearchSourceBuilder searchSourceBuilder, ESPageRequest esPageRequest, ESSort esSort) {
        // 搜索
        SearchResponse searchResponse = getSearchResponse(searchSourceBuilder, esPageRequest, esSort);
        if (searchResponse == null) {
            return null;
        }
        SearchHits searchHits = searchResponse.getHits();
        SearchHit[] hits = searchHits.getHits();
        List<T> genericInstanceList = new ArrayList<>();
        Arrays.stream(hits).forEach(hit -> {
            String sourceAsString = hit.getSourceAsString();
            T object = JSON.parseObject(sourceAsString, genericClass);
            if (getIdValue(object) == null) {
                setIdValue(object, hit.getId());
            }
            genericInstanceList.add(object);
        });

        TotalHits totalHits = searchHits.getTotalHits();
        long total = totalHits.value;
        return new ESPageResult<>(
                total,
                esPageRequest != null ? esPageRequest.getPageNo() : -1,
                esPageRequest != null ? esPageRequest.getSize() : -1,
                genericInstanceList);

    }

    public ESPageResult<ESBaseEntity<T>> searchBase(SearchSourceBuilder searchSourceBuilder, ESPageRequest esPageRequest, ESSort esSort) {
        SearchResponse searchResponse = getSearchResponse(searchSourceBuilder, esPageRequest, esSort);
        if (searchResponse == null) {
            return null;
        }
        SearchHits searchHits = searchResponse.getHits();
        SearchHit[] hits = searchHits.getHits();
        List<ESBaseEntity<T>> genericInstanceList = new ArrayList<>();
        Arrays.stream(hits).forEach(hit -> {
            String sourceAsString = hit.getSourceAsString();
            T object = JSON.parseObject(sourceAsString, genericClass);
            if (getIdValue(object) == null) {
                setIdValue(object, hit.getId());
            }
            ESBaseEntity<T> entity = new ESBaseEntity<>();
            entity.setSortValues(hit.getSortValues());
            entity.setScore(hit.getScore());
            entity.setIndexId(hit.getId());
            entity.setHitIndex(hit.getIndex());
            entity.setData(object);
            genericInstanceList.add(entity);
        });
        TotalHits totalHits = searchHits.getTotalHits();
        long total = totalHits.value;
        return new ESPageResult<>(
                total,
                esPageRequest != null ? esPageRequest.getPageNo() : -1,
                esPageRequest != null ? esPageRequest.getSize() : -1,
                genericInstanceList);

    }

    private SearchResponse getSearchResponse(SearchSourceBuilder searchSourceBuilder, ESPageRequest esPageRequest, ESSort esSort) {
        // 搜索
        Assert.notNull(searchSourceBuilder, "searchSourceBuilder is null");
        SearchRequest searchRequest = new SearchRequest(getIndexNamePattern());
        searchRequest.source(searchSourceBuilder);
        // 分页
        if (esPageRequest != null) {
            searchSourceBuilder.from((esPageRequest.getPageNo() - 1) * esPageRequest.getSize());
            searchSourceBuilder.size(esPageRequest.getSize());
        }
        // 排序
        if (esSort != null) {
            List<SortBuilder> orders = esSort.getOrders();
            if (!CollectionUtils.isEmpty(orders)) {
                orders.forEach(searchSourceBuilder::sort);
            }
            String preference = esSort.getPreference();
            if (StringUtils.hasText(preference)) {
                searchRequest.preference(preference);
            }
        }
        try {
            return getClient().search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private String getIdValue(T genericInstance) {
        if (idField != null) {
            Object idValue = null;
            try {
                idValue = idField.get(genericInstance);
            } catch (IllegalAccessException ignored) {
            }
            if (idValue != null) {
                return idValue.toString();
            }
        }
        return null;
    }

    private void setIdValue(T genericInstance, String value) {
        if (idField != null) {
            try {
                idField.set(genericInstance, value);
            } catch (IllegalAccessException ignored) {
            }
        }
    }

    public T getById(String idValue) {
        try {
            GetResponse getResponse = getClient().get(new GetRequest().index(getIndexName()).id(idValue), RequestOptions.DEFAULT);
            String sourceAsString = getResponse.getSourceAsString();
            return JSON.parseObject(sourceAsString, genericClass);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public List<T> batchGetById(Collection<String> ids) {
        List<T> result = new ArrayList<>();
        try {
            MultiGetRequest multiGetRequest = new MultiGetRequest();
            for (String id : ids) {
                multiGetRequest.add(getIndexName(), id);
            }
            MultiGetResponse getResponse = getClient().mget(multiGetRequest, RequestOptions.DEFAULT);
            MultiGetItemResponse[] responses = getResponse.getResponses();
            for (MultiGetItemResponse respons : responses) {
                GetResponse response = respons.getResponse();
                T object = JSON.parseObject(response.getSourceAsString(), genericClass);
                if (object != null) {
                    result.add(object);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }


}
