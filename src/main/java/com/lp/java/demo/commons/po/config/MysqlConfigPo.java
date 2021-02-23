package com.lp.java.demo.commons.po.config;

import java.io.Serializable;

/**
 * @author li.pan
 * @version 1.0.0
 * @Description Mysql配置项
 * @createTime 2021年02月22日 15:52:00
 */
public class MysqlConfigPo implements Serializable {

    private static final long serialVersionUID = 6248517349213721967L;
    /**
     * url地址
     */
    public static String url;

    /**
     * driver驱动
     */
    public static String driver;

    /**
     * 线程池中最大size
     */
    public static Integer maxPoolSize;

    /**
     * 有效时间
     */
    public static Integer maxIdleTime;

    /**
     * 用户
     */
    public static String user;

    /**
     * 密码
     */
    public static String password;

}
