package com.github.orm.elasticsearch.core.base;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @ClassName GEOLocation
 * @Deacription
 * @Author liyongbing
 * @Date 2020/7/22 17:11
 * @Version 1.0
 **/
@NoArgsConstructor
@AllArgsConstructor
@Data
public class GEOLocation implements Serializable {
    private Double lat;
    private Double lon;

    /**
     * 检查坐标是否合法
     * -90 < lat < 90
     * -180 < lon < 180
     *
     * @return
     */
    private static boolean checkLoLa(Double lat, Double lon) {
        return Math.abs(lat) < 90 && Math.abs(lon) < 180;
    }

    /**
     * 安全获取坐标对象，非法数据直接返回 null
     *
     * @return
     */
    public static GEOLocation getInstance(Double lat, Double lon) {
        return checkLoLa(lat, lon) ? new GEOLocation(lat, lon) : null;
    }

}
