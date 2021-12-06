package com.lp.java.demo.table.sql.time;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author:cbxg
 * @date:2021/4/7
 * @description: sql中使用时间语义
 */
public class ProcessTime02Sql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql("create table sensor (id string ,ts bigint, vc int, pt as  PROCTIME()) with (" +
                "'connector' = 'filesystem'," +
                "'path'='D:\\gitProjects\\flink_sql_tutorials\\src\\main\\resources\\sensor.csv'," +
                "'format' = 'csv' " +
                ")");
        TableResult tableResult = tableEnv.executeSql("select * from sensor");
        tableResult.print();

    }
}
