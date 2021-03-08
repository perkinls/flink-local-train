package com.lp.java.demo.datastream.process;

import com.lp.java.demo.datastream.richfunction.RichMapSplit2KV;
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
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class BroadcastKeyed {

    private final static Logger log = LoggerFactory.getLogger(BroadcastNoneKeyed.class);

    public static void main(String[] args) throws Exception {

        delFile("/Users/meitu/Desktop/1");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "mt-mdh.local:9093");
        properties.setProperty("group.id", "BroadcastKeyed");

        FlinkKafkaConsumer<String> kafkaConsumer010 = new FlinkKafkaConsumer<>("KV",
                new SimpleStringSchema(),
                properties);

        kafkaConsumer010.setStartFromLatest();

        KeyedStream<Tuple2<String, Long>, org.apache.flink.api.java.tuple.Tuple> keyBy = env
                .addSource(kafkaConsumer010)
                .map(new RichMapSplit2KV())
                .keyBy(0);

        MapStateDescriptor<String, String> ruleStateDescriptor = new MapStateDescriptor<>(
                "RulesBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO
        );

        BroadcastStream<Tuple2<String, String>> broadcast = env.fromCollection(getBroadcastData()).broadcast(ruleStateDescriptor);


        SingleOutputStreamOperator<Tuple3<String, String, Long>> res = keyBy.connect(broadcast).
                process(new KeyedBroadcastProcessFunction<String, Tuple2<String, Long>, Tuple2<String, String>, Tuple3<String, String, Long>>() {
                    private static final long serialVersionUID = 3084148678696546522L;

                    MapStateDescriptor<String, String> ruleStateDescriptor = new MapStateDescriptor<>(
                            "RulesBroadcastState",
                            BasicTypeInfo.STRING_TYPE_INFO,
                            BasicTypeInfo.STRING_TYPE_INFO
                    );

                    @Override
                    public void processElement(Tuple2<String, Long> value, ReadOnlyContext ctx, Collector<Tuple3<String, String, Long>> out) throws Exception {
                        ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(ruleStateDescriptor);
                        out.collect(new Tuple3<>(value.f0, broadcastState.get(value.f0), value.f1));
                    }

                    @Override
                    public void processBroadcastElement(Tuple2<String, String> value, Context ctx, Collector<Tuple3<String, String, Long>> out) throws Exception {
                        ctx.getBroadcastState(ruleStateDescriptor).put(value.f0, value.f1);
                    }
                });

        res.writeAsText("/Users/meitu/Desktop/1");

        // execute the program
        env.execute("Iterative Pi Example");
    }

    public static List<Tuple2<String, String>> getBroadcastData() {
        List<Tuple2<String, String>> data = new ArrayList<>();

        data.add(new Tuple2<>("apple", "red"));
        data.add(new Tuple2<>("pear", "white"));
        data.add(new Tuple2<>("nut", "black"));
        data.add(new Tuple2<>("grape", "orange"));
        data.add(new Tuple2<>("banana", "yellow"));
        data.add(new Tuple2<>("pineapple", "purple"));
        data.add(new Tuple2<>("pomelo", "blue"));
        data.add(new Tuple2<>("orange", "ching"));
        return data;
    }


    public static void delFile(String path) {
        File file = new File(path);
        if (file.exists() && file.isFile()) {
            log.info("删除文件Path: {}", path);
            file.delete();
        }
    }
}
