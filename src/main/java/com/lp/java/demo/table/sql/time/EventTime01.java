//package com.lp.java.demo.table.sql.time;
//
//import com.cbxg.sql.bean.WaterSensor;
//import com.sun.org.apache.xerces.internal.impl.io.UCSReader;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import org.apache.log4j.Level;
//import org.apache.log4j.Logger;
//
//import java.time.Duration;
//
//import static org.apache.flink.table.api.Expressions.$;
//
///**
// * @author:cbxg
// * @date:2021/4/7
// * @description: 事件时间语义的列子
// */
//public class EventTime01 {
//    public static void main(String[] args) {
//        Logger.getLogger("org").setLevel(Level.INFO);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//
//        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStream = env.fromElements(new WaterSensor("sensor_01", 10000L, 10),
//                new WaterSensor("sensor_01", 1000000L, 10),
//                new WaterSensor("sensor_01", 10000000L, 10),
//                new WaterSensor("sensor_01", 5000L, 10),
//                new WaterSensor("sensor_01", 1000000L, 10))
//                .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
//                        .withTimestampAssigner((element, s) -> element.getTs()));
//
//        Table table = tableEnv.fromDataStream(waterSensorSingleOutputStream,
//                $("id"),$("ts").rowtime(),$("vc"));
//
//        table.execute().print();
//
//
//    }
//}
