package com.lp.java.demo.datastream.broadcast;

import com.lp.java.demo.datastream.broadcast.Util.FileUtil;
import com.lp.java.demo.datastream.broadcast.Util.PrepareBroadCastData;
import com.lp.java.demo.datastream.util.Split2KV;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;


import java.util.Properties;

public class BroadcastKeyed {
    public static void main(String[] args) throws Exception {

        FileUtil.delFile("/Users/meitu/Desktop/1");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "mt-mdh.local:9093");
        properties.setProperty("group.id", "BroadcastKeyed");

        FlinkKafkaConsumer010<String> kafkaConsumer010 = new FlinkKafkaConsumer010<>("KV",
                new SimpleStringSchema(),
                properties);

        kafkaConsumer010.setStartFromLatest();

        KeyedStream<Tuple2<String, Long>, org.apache.flink.api.java.tuple.Tuple> keyBy = env
                .addSource(kafkaConsumer010)
                .map(new Split2KV())
                .keyBy(0);

        MapStateDescriptor<String, String> ruleStateDescriptor = new MapStateDescriptor<>(
                "RulesBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO
                );

        BroadcastStream<Tuple2<String, String>> broadcast = env.fromCollection(PrepareBroadCastData.getBroadcastData()).broadcast(ruleStateDescriptor);


        SingleOutputStreamOperator<Tuple3<String, String, Long>> res = keyBy.connect(broadcast).
                process(new KeyedBroadcastProcessFunction<String, Tuple2<String,Long>, Tuple2<String,String>, Tuple3<String,String,Long>>() {
            MapStateDescriptor<String, String> ruleStateDescriptor = new MapStateDescriptor<>(
                    "RulesBroadcastState",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO
            );
            @Override
            public void processElement(Tuple2<String, Long> value, ReadOnlyContext ctx, Collector<Tuple3<String, String, Long>> out) throws Exception {
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(ruleStateDescriptor);
                out.collect(new Tuple3<>(value.f0, broadcastState.get(value.f0),value.f1));
            }
            @Override
            public void processBroadcastElement(Tuple2<String, String> value, Context ctx, Collector<Tuple3<String, String, Long>> out) throws Exception {
                ctx.getBroadcastState(ruleStateDescriptor).put(value.f0,value.f1);
            }
        });

        res.writeAsText("/Users/meitu/Desktop/1");

        // execute the program
        env.execute("Iterative Pi Example");
    }
}
