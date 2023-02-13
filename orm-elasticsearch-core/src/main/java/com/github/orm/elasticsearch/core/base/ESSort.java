package com.github.orm.elasticsearch.core.base;

import lombok.Getter;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author liyongbing
 * @Date 2020/07/23
 * 封装排序参数
 */
public class ESSort {

    @Getter
    private List<SortBuilder> orders;
    //_only_local
    //Run the search only on shards on the local node.
    //_local
    //If possible, run the search on shards on the local node. If not, select shards using the default method.
    //_only_nodes:<node-id>,<node-id>
    //Run the search on only the specified nodes IDs. If suitable shards exist on more than one selected node, use shards on those nodes using the default method. If none of the specified nodes are available, select shards from any available node using the default method.
    //_prefer_nodes:<node-id>,<node-id>
    //If possible, run the search on the specified nodes IDs. If not, select shards using the default method.
    //_shards:<shard>,<shard>
    //Run the search only on the specified shards. You can combine this value with other preference values, excluding <custom-string>. However, the _shards value must come first. For example: _shards:2,3|_local.
    //<custom-string>
    //Any string that does not start with _. If the cluster state and selected shards do not change, searches using the same <custom-string> value are routed to the same shards in the same order.
    @Getter
    private String preference;

    public ESSort() {
        orders = new ArrayList<>();
    }

    public ESSort(SortOrder direction, String property) {
        orders = new ArrayList<>();
        add(direction, property);
    }

    public ESSort setPreference(String preference) {
        this.preference = preference;
        return this;
    }

    /**
     * 追加排序字段
     *
     * @param direction 排序方向
     * @param property  排序字段
     * @return
     */
    public ESSort add(SortOrder direction, String property) {
        Assert.notNull(direction, "direction must not be null!");
        Assert.hasText(property, "fieldName must not be empty!");
        orders.add(SortBuilders.fieldSort(property).order(direction));
        return this;
    }

    /**
     * 自定义排序
     *
     * @param sortBuilder
     * @return
     */
    public ESSort add(SortBuilder<?> sortBuilder) {
        Assert.notNull(sortBuilder, "sortBuilder must not be null!");
        orders.add(sortBuilder);
        return this;
    }

}
