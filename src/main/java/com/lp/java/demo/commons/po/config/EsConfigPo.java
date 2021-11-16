package com.lp.java.demo.commons.po.config;

import java.io.Serializable;

/**
 * @author li.pan
 * @version 1.0.0
 * @Description ES 参数配置类
 * @createTime 2021年01月22日 12:54:00
 */
public class EsConfigPo implements Serializable {

    private static final long serialVersionUID = -461538067320569418L;

    /**
     * 集群名称
     */
    public static String clusterName;
    /**
     * 集群节点
     */
    public static String clusterServers;
    /**
     * 是否开启用户名密码认证
     */
    public static Boolean authEnabled;
    /**
     * 用户名
     */
    public static String authUsername;
    /**
     * 密码
     */
    public static String authPassword;

    /**
     * 是否开启kerberos认证
     */
    public static Boolean krbEnabled;
    /**
     * principal用户
     */
    public static String krbPrincipal;
    /**
     * krb5文件
     */
    public static String Krb5File;
    /**
     * keytab文件
     */
    public static String krbKeytabFile;

}
