package com.lp.java.demo.table.sql.windows;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;

/**
 * @author:cbxg
 * @date:2021/4/10
 * @description: 滚动窗口sql语法示例
 */
public class TumbelWindowSQL {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql("create table sensor (" +
                "id string ," +
                "ts bigint ," +
                "vc int ," +
                "t as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss')),"+
                "watermark for t as t - interval '5' second " +
                ") with(" +
                "'connector' = 'filesystem'," +
                "'format' = 'csv'," +
                "'path' = 'D:\\gitProjects\\flink_sql_tutorials\\src\\main\\resources\\sensor.csv'" +
                ")");

        tableEnv.sqlQuery("select id," +
                " TUMBLE_START(t, INTERVAL '1' minute) as wStart," +
                " TUMBLE_END(t, INTERVAL '1' minute) as wEnd," +
                " sum(vc)" +
                " from sensor" +
                " GROUP BY TUMBLE(t, INTERVAL '1' minute),id").execute().print();





    }
}
