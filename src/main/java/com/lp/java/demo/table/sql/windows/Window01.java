//package com.lp.java.demo.table.sql.windows;
//
//import com.cbxg.sql.bean.WaterSensor;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.Tumble;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//
//import java.time.Duration;
//
//import static org.apache.flink.table.api.Expressions.$;
//import static org.apache.flink.table.api.Expressions.lit;
//
///**
// * @author:cbxg
// * @date:2021/4/7
// * @description: 聚合窗口简单例子
// */
//public class Window01 {
//    public static void main(String[] args) throws Exception {
//        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
//        environment.setParallelism(1);
//        final StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
//
//        final SingleOutputStreamOperator<WaterSensor> waterSensorStream = environment.fromElements(new WaterSensor("sensor_01", 10000L, 10),
//                new WaterSensor("sensor_01", 10000L, 10),
//                new WaterSensor("sensor_02", 20000L, 10),
//                new WaterSensor("sensor_03", 30000L, 10),
//                new WaterSensor("sensor_01", 20000L, 10),
//                new WaterSensor("sensor_01", 32000L, 10),
//                new WaterSensor("sensor_01", 1000L, 10),
//                new WaterSensor("sensor_01", 1000L, 10),
//                new WaterSensor("sensor_01", 1000L, 10)
//        ).assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness((Duration.ofSeconds(3)))
//                .withTimestampAssigner(((element, recordTimestamp) -> element.getTs())));
//
//        final Table table = tableEnvironment.fromDataStream(waterSensorStream, $("id"), $("ts").rowtime(), $("vc"));
//        table.window(Tumble.over(lit(10).second()).on($("ts")).as("w"))
//                .groupBy($("id"),$("w"))
//                .select($("id"),$("w").start(),$("w").end(),$("vc").sum())
//                .execute()
//                .print();
//
//        environment.execute();
//
//
//    }
//}
