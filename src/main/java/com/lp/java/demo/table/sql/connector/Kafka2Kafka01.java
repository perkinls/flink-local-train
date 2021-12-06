package com.lp.java.demo.table.sql.connector;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author:cbxg
 * @date:2021/4/6
 * @description: 使用sql消费kafka数据到kafka中
 */
public class Kafka2Kafka01 {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new FsStateBackend("file:///D:/gitProjects/flink_sql_tutorials/chk"));
        //每1分钟的检查点
        env.enableCheckpointing(2*60 * 1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

//        source kafka topic 注册为表
        // {"id":"sensor_01","ts":10000,"va":10}
        tableEnv.executeSql("create table source_sensor(id string , ts bigint , vc int) with(" +
                "'connector' = 'kafka'," +
                "'topic' = 'sensor'," +
                "'properties.group.id' = 'source_sensor'," +
                "'properties.bootstrap.servers' = 'hadoop102:9092'," +
                "'scan.startup.mode' = 'group-offsets'," +
                "'format' = 'json' " +
                ")");
//        sink kafka topic 注册为表
        tableEnv.executeSql("create table sink_sensor(id string , ts bigint , vc int) with(" +
                "'connector' = 'kafka'," +
                "'topic' = 'sink_sensor'," +
                "'properties.bootstrap.servers' = 'hadoop102:9092'," +
                "'format' = 'json' " +
                ")");

        tableEnv.executeSql("insert into sink_sensor select * from source_sensor where id ='sensor_01'");
    }
}
