package com.github.orm.elasticsearch.core.setting;

import org.elasticsearch.common.xcontent.XContentBuilder;

/**
 * @ClassName ESClassSetting
 * @Description
 * @Author liyongbing
 * @Date 2022/9/13 11:31
 * @Version 1.0
 **/
public interface ESClassSetting {

    /**
     * 设置setting对应type
     * @param type
     */
    ESClassSetting setDocType(Class<?> type) throws Exception;

    /**
     * 设置settings
     * @return
     * @throws Exception
     */
    ESClassSetting setting() throws Exception;

    /**
     * 获取setting生成builder
     * @return
     * @throws Exception
     */
    XContentBuilder getBuilder() throws Exception;

}
