package com.lp.java.demo.commons.po.config;

import java.io.Serializable;

/**
 * @author li.pan
 * @version 1.0.0
 * @Description Redis配置项
 * @createTime 2021年02月22日 15:53:00
 */
public class RedisConfigPo implements Serializable {
    private static final long serialVersionUID = -7736179571195661759L;
    /**
     * 主机
     */
    public static String host;
    /**
     * 端口
     */
    public static Integer port;
}
