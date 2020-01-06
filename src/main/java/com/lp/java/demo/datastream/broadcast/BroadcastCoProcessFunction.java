package com.lp.java.demo.datastream.broadcast;

import com.lp.java.demo.datastream.broadcast.Util.FileUtil;
import com.lp.java.demo.datastream.broadcast.Util.PrepareBroadCastData;
import com.lp.java.demo.datastream.util.Split2KV;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;


import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class BroadcastCoProcessFunction {
    public static void main(String[] args) throws Exception {

        FileUtil.delFile("/Users/meitu/Desktop/1");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "mt-mdh.local:9093");
        properties.setProperty("group.id", "BroadcastCoProcessFunction");

        FlinkKafkaConsumer010<String> kafkaConsumer010 = new FlinkKafkaConsumer010<>("KV",
                new SimpleStringSchema(),
                properties);

        kafkaConsumer010.setStartFromLatest();

        KeyedStream<Tuple2<String, Long>, org.apache.flink.api.java.tuple.Tuple> keyBy = env
                .addSource(kafkaConsumer010)
                .map(new Split2KV())
                .keyBy(0);

        DataStream<Tuple2<String, String>> broadcast = env.fromCollection(PrepareBroadCastData.getBroadcastData()).broadcast();

        SingleOutputStreamOperator<Tuple3<String, String, Long>> res = keyBy.connect(broadcast).process(new CoProcessFunction<Tuple2<String, Long>, Tuple2<String, String>, Tuple3<String, String, Long>>() {

            private Map<String,String> rule = new HashMap<>();
            @Override
            public void processElement1(Tuple2<String, Long> value, Context ctx, Collector<Tuple3<String, String, Long>> out) throws Exception {
                out.collect(new Tuple3<>(value.f0,rule.get(value.f0),value.f1));
            }

            @Override
            public void processElement2(Tuple2<String, String> value, Context ctx, Collector<Tuple3<String, String, Long>> out) throws Exception {
                rule.put(value.f0,value.f1);
            }
        });

        res.writeAsText("/Users/meitu/Desktop/1");

        // execute the program
        env.execute("Iterative Pi Example");
    }
}
