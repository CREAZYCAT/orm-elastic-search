package com.github.orm.elasticsearch.core.base;


/**
 * 分页
 *
 * @author liyongbing
 * @date 2020/07/23
 */
public class ESPageRequest {

    private final int pageNo;
    private final int size;

    public ESPageRequest(Long pageNo, Long size) {
        this(pageNo.intValue(),size.intValue());
    }

    public ESPageRequest(int pageNo, int size) {

        if (pageNo < 1) {
            throw new IllegalArgumentException("Page index must not be less than zero!");
        }

        if (size < 1) {
            throw new IllegalArgumentException("Page size must not be less than one!");
        }

        this.pageNo = pageNo;
        this.size = size;
    }

    public int getPageNo() {
        return pageNo;
    }

    public int getSize() {
        return size;
    }
}

