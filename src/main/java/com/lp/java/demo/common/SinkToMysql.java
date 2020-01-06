package com.lp.java.demo.common;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * <p/>
 * <li>title: DataStream Sink</li>
 * <li>@author: li.pan</li>
 * <li>Date: 2019/12/29 5:00 下午</li>
 * <li>Version: V1.0</li>
 * <li>Description: Sink写出到Mysql</li>
 */
public class SinkToMysql extends RichSinkFunction<Student>{
    Connection connection;
    PreparedStatement ps;


    private Connection getConnection() {
        Connection conn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");

            String url = "jdbc:mysql://localhost:3306/flink_demo";
            conn = DriverManager.getConnection(url,"root","1234");

        } catch (Exception e) {
            e.printStackTrace();
        }

        return conn;
    }

    /**
     * 在open方法中建立connection
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        connection = getConnection();
        String sql = "insert into student(id,name,age) values (?,?,?)";
        ps = connection.prepareStatement(sql);


        System.out.println("open");

    }

    // 每条记录插入时调用一次
    @Override
    public void invoke(Student value, Context context) throws Exception {
        System.out.println("invoke~~~~~~~~~");
        // 未前面的占位符赋值
        ps.setInt(1, value.getId());
        ps.setString(2, value.getName());
        ps.setInt(3, value.getAge());

        ps.executeUpdate();

    }

    /**
     * 在close方法中要释放资源
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();

        if(ps != null) {
            ps.close();
        }

        if(connection != null) {
            connection.close();
        }
    }

}
