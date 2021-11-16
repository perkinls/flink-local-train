package com.lp.java.demo.commons.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/**
 * @author li.pan
 * @version 1.0.0
 * @title HBase连接工具类
 * @createTime 2021年03月10日 22:30:00
 */
public class HBaseUtils {

    /**
     * @param zkQuorum zookeeper地址，多个要用逗号分隔
     * @param port     zookeeper端口号
     * @return connection
     * @ 简单的HBase连接方法,对于不同的集群kennel需要添加不同的配置
     */
    public static Connection getConnection(String zkQuorum, int port) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zkQuorum);
        conf.set("hbase.zookeeper.property.clientPort", port + "");

        Connection connection = ConnectionFactory.createConnection(conf);
        return connection;
    }
}
