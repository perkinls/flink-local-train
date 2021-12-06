//package com.lp.java.demo.table.sql.windows;
//
//import com.cbxg.sql.bean.WaterSensor;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.GroupWindowedTable;
//import org.apache.flink.table.api.Slide;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//
//import java.time.Duration;
//
//import static org.apache.flink.table.api.Expressions.$;
//import static org.apache.flink.table.api.Expressions.lit;
//
///**
// * @author:cbxg
// * @date:2021/4/10
// * @description: 滚动窗口示例
// */
//public class HopWinodw {
//    public static void main(String[] args) {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//        SingleOutputStreamOperator<WaterSensor> sensorStream = env.fromElements(
//                new WaterSensor("sensor_01", 10000L, 10),
//                new WaterSensor("sensor_02", 20000L, 10),
//                new WaterSensor("sensor_03", 30000L, 10),
//                new WaterSensor("sensor_01", 20000L, 10),
//                new WaterSensor("sensor_01", 32000L, 10),
//                new WaterSensor("sensor_01", 12000L, 10),
//                new WaterSensor("sensor_01", 15000L, 10),
//                new WaterSensor("sensor_01", 17000L, 10))
//                .assignTimestampsAndWatermarks(WatermarkStrategy
//                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
//                        .withTimestampAssigner(((element, recordTimestamp) -> element.getTs())));
//        Table table = tableEnv.fromDataStream(sensorStream, $("id"), $("ts").rowtime(), $("vc"));
//        table.window(Slide.over(lit(10).second()).every(lit(5).second()).on($("ts")).as("w"))
//                .groupBy($("id"),$("w"))
//                .select($("id"),$("w").start(),$("w").end(),$("vc").sum())
//                .execute()
//                .print();
//    }
//}
