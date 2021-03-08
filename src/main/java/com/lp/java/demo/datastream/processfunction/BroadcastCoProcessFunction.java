package com.lp.java.demo.datastream.processfunction;

import com.lp.java.demo.commons.BaseStreamingEnv;
import com.lp.java.demo.commons.IBaseRunApp;
import com.lp.java.demo.commons.po.config.KafkaConfigPo;
import com.lp.java.demo.datastream.processfunction.util.FileUtil;
import com.lp.java.demo.datastream.processfunction.util.PrepareBroadCastData;
import com.lp.java.demo.datastream.processfunction.util.Split2KV;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;


import java.util.HashMap;
import java.util.Map;

public class BroadcastCoProcessFunction extends BaseStreamingEnv<String> implements IBaseRunApp {
    @Override
    public void doMain() throws Exception {
        FileUtil.delFile("/Users/meitu/Desktop/1");
        FlinkKafkaConsumer<String> consumer = getKafkaConsumer(KafkaConfigPo.kvTopic1, new SimpleStringSchema());

        KeyedStream<Tuple2<String, Long>, String> keyByStream = env
                .addSource(consumer)
                .map(new Split2KV())
                .keyBy((KeySelector<Tuple2<String, Long>, String>) value -> value.f0);

        // 注册广播
        DataStream<Tuple2<String, String>> broadcast =
                env.fromCollection(PrepareBroadCastData.getBroadcastData()).broadcast();

        SingleOutputStreamOperator<Tuple3<String, String, Long>> res =
                keyByStream
                        //两个数据流被Connect之后，只是被放在了一个同一个流中，内部依然保持各自的数据和形式不发生任何变化，两个流相互独立。
                        .connect(broadcast)
                        .process(new CoProcessFunction<Tuple2<String, Long>, Tuple2<String, String>, Tuple3<String, String, Long>>() {

                            private static final long serialVersionUID = 4468906271625927719L;

                            private Map<String, String> rule = new HashMap<>();

                            @Override
                            public void processElement1(Tuple2<String, Long> value, Context ctx, Collector<Tuple3<String, String, Long>> out) throws Exception {
                                out.collect(new Tuple3<>(value.f0, rule.get(value.f0), value.f1));
                            }

                            @Override
                            public void processElement2(Tuple2<String, String> value, Context ctx, Collector<Tuple3<String, String, Long>> out) throws Exception {
                                rule.put(value.f0, value.f1);
                            }
                        });

        res.writeAsText("/Users/meitu/Desktop/1");

        // execute the program
        env.execute("Iterative Pi Example");

    }


}
