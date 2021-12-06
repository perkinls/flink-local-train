package com.lp.java.demo.datastream.process;

import com.lp.java.demo.datastream.BaseStreamingEnv;
import com.lp.java.demo.base.IBaseRunApp;
import com.lp.java.demo.commons.po.config.JobConfigPo;
import com.lp.java.demo.commons.po.config.KafkaConfigPo;
import com.lp.java.demo.datastream.richfunction.RichMapSplit2KV;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author li.pan
 * @version 1.0.0
 * @title 用于connect的低价函数 CoProcessFunction
 * @createTime 2021年03月08日 13:36:00
 * 低价函数: { http://www.lllpan.top/article/85 }
 *
 * DataStream,DataStream → ConnectedStreams:连接两个保持他们类型的数据流，
 * 两个数据流被 Connect 之后，只是被放在了一个同一个流中，内部依然保持 各自的数据和形式不发生任何变化，两个流相互独立。
 */
public class ProcessFunctionConnectCo extends BaseStreamingEnv<String> implements IBaseRunApp {
    private static final Logger log = LoggerFactory.getLogger(ProcessFunctionConnectCo.class);


    @Override
    protected Integer setDefaultParallelism() {
        return 1;
    }

    @Override
    protected void setKafkaFromOffsets(FlinkKafkaConsumer<String> consumer) {
        consumer.setStartFromLatest();
    }

    private static List<Tuple2<String, String>> getBroadcastData() {
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

    @Override
    public void doMain() throws Exception {

        // 注册广播
        DataStream<Tuple2<String, String>> broadcast = env.fromCollection(getBroadcastData()).broadcast();

        FlinkKafkaConsumer<String> consumer = getKafkaConsumer(KafkaConfigPo.kvTopic1, new SimpleStringSchema());

        KeyedStream<Tuple2<String, Long>, String> keyByStream = env
                .addSource(consumer)
                .map(new RichMapSplit2KV())
                .keyBy((KeySelector<Tuple2<String, Long>, String>) value -> value.f0);


        SingleOutputStreamOperator<Tuple3<String, String, Long>> res =
                keyByStream
                        //两个数据流被Connect之后，只是被放在了一个同一个流中，内部依然保持各自的数据和形式不发生任何变化，两个流相互独立。
                        .connect(broadcast)
                        .process(new CoProcessFunction<Tuple2<String, Long>, Tuple2<String, String>, Tuple3<String, String, Long>>() {

                            private static final long serialVersionUID = 4468906271625927719L;
                            private Map<String, String> rule = new HashMap<>();

                            @Override
                            public void processElement1(Tuple2<String, Long> value, Context ctx, Collector<Tuple3<String, String, Long>> out) throws Exception {
                                log.info("==============================processElement1 value: {}==============================", value);
                                out.collect(new Tuple3<>(value.f0, rule.get(value.f0), value.f1));
                            }

                            @Override
                            public void processElement2(Tuple2<String, String> value, Context ctx, Collector<Tuple3<String, String, Long>> out) throws Exception {
                                log.info("==============================processElement2 value: {}==============================", value);
                                rule.put(value.f0, value.f1);
                            }


                        });


        res.print("Connect ProcessFunction Result:");

        // execute the program
        env.execute(JobConfigPo.jobNamePrefix + ProcessFunctionConnectCo.class.getName());

    }



}
