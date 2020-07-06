package com.lp.java.demo.datastream.sink.mysql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;


/**
 * <p/>
 * <li>title: 自定义 Sink</li>
 * <li>@author: li.pan</li>
 * <li>Date: 2020/1/16 1:32 下午</li>
 * <li>Version: V1.0</li>
 * <li>Description: 自定义Mysql Sink</li>
 */
public class CustomJdbcSink extends RichSinkFunction<Integer> {

    private static final long serialVersionUID = 1L;

    private Connection connection;
    private PreparedStatement preparedStatement;

    /**
     * open方法是初始化方法，会在invoke方法之前执行，执行一次。
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        // JDBC连接信息
        String USERNAME = "root";
        String PASSWORD = "1234";
        String DBURL = "jdbc:mysql://localhost:3306/test";
        // 加载JDBC驱动
        Class.forName("com.mysql.jdbc.Driver");
        // 获取数据库连接
        connection = DriverManager.getConnection(DBURL, USERNAME, PASSWORD);
        String sql = "insert into flink_test (sum_value) values (?)";
        preparedStatement = connection.prepareStatement(sql);
        super.open(parameters);
    }

    @Override
    public void invoke(Integer value, Context context) throws Exception {
        try {
            preparedStatement.setInt(1, value);
            preparedStatement.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * close()是tear down的方法，在销毁时执行，关闭连接。
     */
    @Override
    public void close() throws Exception {
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
        super.close();
    }
}
