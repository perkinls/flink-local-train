package com.lp.java.demo.table.sql.time;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author:cbxg
 * @date:2021/4/7
 * @description: 事件时间语义例子 sql写法
 */
public class EventTime02sql {
    public static void main(String[] args) {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        final StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        tableEnvironment.executeSql("create table sensor(" +
                "id string," +
                "ts bigint," +
                "vc int," +
                "t as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
                "watermark for t as t - interval '5' second )" +
                "with (" +
                "'connector' = 'filesystem'," +
                "'path' = 'D:\\gitProjects\\flink_sql_tutorials\\src\\main\\resources\\sensor.csv'," +
                "'format' = 'csv'" +
                ")");

        tableEnvironment.sqlQuery("select * from sensor").execute().print();
    }
}
