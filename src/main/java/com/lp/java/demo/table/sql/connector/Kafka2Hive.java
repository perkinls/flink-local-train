//package com.lp.java.demo.table.sql.connector;
//
//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.JSONObject;
//import com.cbxg.sql.bean.Person;
//import com.cbxg.sql.bean.WaterSensor;
//import lombok.AllArgsConstructor;
//import lombok.Data;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.runtime.state.filesystem.FsStateBackend;
//import org.apache.flink.streaming.api.CheckpointingMode;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.DataTypes;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import org.apache.flink.table.catalog.hive.HiveCatalog;
//import org.apache.flink.table.descriptors.Json;
//import org.apache.flink.table.descriptors.Kafka;
//import org.apache.flink.table.descriptors.Schema;
//import org.apache.kafka.clients.consumer.ConsumerConfig;
//
//import java.util.Properties;
//import java.util.Random;
//
///**
// * @author:cbxg
// * @date:2021/7/20
// * @description:
// */
//public class Kafka2Hive {
//    public static void main(String[] args) {
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setStateBackend(new FsStateBackend("file:///D:/gitProjects/flink_sql_tutorials/chk"));
//        //每1分钟的检查点
//        env.enableCheckpointing(2*60 * 1000);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//
//        env.setParallelism(2);
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//
//
//        Properties sourceKafkaProp = new Properties();
//        sourceKafkaProp.setProperty("bootstrap.servers", "hadoop102:9092");
//        sourceKafkaProp.setProperty("group.id", "sql01_test_01");
//        String sourceTopic = "sql_test1";
//
//        org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer<String> kafkaConsumer = new org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer<>(sourceTopic,
//                new SimpleStringSchema(),
//                sourceKafkaProp);
//
//        SingleOutputStreamOperator<String> source = env.addSource(kafkaConsumer).name("source").uid("source");
//
//        String catalogName = "hiveCatalog01";
//        String database = "test1";
//        String hiveConfDir = "D:/gitProjects/flink_sql_tutorials/src/main/resources/conf";
//        HiveCatalog hiveCatalog = new HiveCatalog(catalogName, database, hiveConfDir);
//        tableEnv.registerCatalog(catalogName,hiveCatalog);
//        tableEnv.useCatalog(catalogName);
//        tableEnv.useDatabase(database);
////        tableEnv.sqlQuery("select * from ods_sku_info limit 10").execute().print();
//
////        source kafka topic 注册为表
//        // {"id":"sensor_01","ts":10000,"va":10}
//
////        DataStreamSource<WaterSensor> waterSensorStream =
////                env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
////                        new WaterSensor("sensor_1", 2000L, 20),
////                        new WaterSensor("sensor_2", 3000L, 30),
////                        new WaterSensor("sensor_1", 4000L, 40),
////                        new WaterSensor("sensor_1", 5000L, 50),
////                        new WaterSensor("sensor_2", 6000L, 60));
////        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 9750);
//
//        SingleOutputStreamOperator<Person> map = source.map(new MapFunction<String, Person>() {
//            @Override
//            public Person map(String s) throws Exception {
//                return new Person(s,new Random().nextInt(100),new WaterSensor("sensor_1", 4000L, 40));
//            }
//        });
//
//        // 使用sql查询一个已注册的表
//        // 1. 从流得到一个表
////        final SingleOutputStreamOperator<Person> map = waterSensorStream.map(v -> new Person(v.getId(), v.getVc(), v));
////        Table inputTable = tableEnv.fromDataStream(waterSensorStream);
////        Table inputTable = tableEnv.fromDataStream(map);
//        // 2. 把注册为一个临时视图
//        tableEnv.createTemporaryView("tmp_person", map);
//        tableEnv.executeSql("insert into person  select name,age,ws , from_unixtime(unix_timestamp(), 'yyyy/MM/dd-HH')  as  dt from tmp_person");
//        tableEnv.executeSql("insert into person1 select name,age,ws , from_unixtime(unix_timestamp(), 'yyyy/MM/dd-HH')  as  dt from tmp_person");
//
//        try {
//            env.execute();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//
////        tableEnv.executeSql("create table source_sensor1(id string , ts bigint , vc int) with(" +
////                "'connector' = 'kafka'," +
////                "'topic' = 'sensor'," +
////                "'properties.group.id' = 'source_sensor'," +
////                "'properties.bootstrap.servers' = 'hadoop102:9092'," +
////                "'scan.startup.mode' = 'group-offsets'," +
////                "'format' = 'json' " +
////                ")");
////        sink kafka topic 注册为表
////        tableEnv.executeSql("create table sink_sensor(id string , ts bigint , vc int) with(" +
////                "'connector' = 'kafka'," +
////                "'topic' = 'sink_sensor'," +
////                "'properties.bootstrap.servers' = 'hadoop102:9092'," +
////                "'format' = 'json' " +
////                ")");
//
////        tableEnv.executeSql("insert into person partition (dt = '2021') select * from tmp_person ");
//    }
//
//}
//
//
//
//
