//package com.lp.java.demo.table.sql.time;
//
//import com.cbxg.sql.bean.WaterSensor;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import org.apache.flink.types.Row;
//
//import static org.apache.flink.table.api.Expressions.$;
//
///**
// * @author:cbxg
// * @date:2021/4/7
// * @description: 时间语义测试
// */
//public class ProcessTime01 {
//
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        DataStreamSource<WaterSensor> streamSource = env.fromElements(new WaterSensor("sensor_01", 100000L, 1000),
//                new WaterSensor("sensor_01", 100000L, 1000),
//                new WaterSensor("sensor_01", 100000L, 1000),
//                new WaterSensor("sensor_01", 100000L, 1000));
//
//
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//        Table table = tableEnv.fromDataStream(streamSource,$("id"), $("ts"), $("vc"), $("pt").proctime());
//        tableEnv.toAppendStream(table, Row.class).print();
//
//        env.execute("ProcessTime01");
//    }
//}
