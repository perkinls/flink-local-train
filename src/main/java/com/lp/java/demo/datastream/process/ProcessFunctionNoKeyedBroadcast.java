package com.lp.java.demo.datastream.process;

import com.lp.java.demo.datastream.BaseStreamingEnv;
import com.lp.java.demo.datastream.IBaseRunApp;
import com.lp.java.demo.commons.po.config.JobConfigPo;
import com.lp.java.demo.commons.po.config.KafkaConfigPo;
import com.lp.java.demo.datastream.richfunction.RichMapSplit2KV;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author li.pan
 * @version 1.0.0
 * @title 用于广播的低价函数 BroadcastProcessFunction
 * @createTime 2021年03月08日 13:36:00
 * 低价函数: http://www.lllpan.top/article/85
 */
public class ProcessFunctionNoKeyedBroadcast extends BaseStreamingEnv<String> implements IBaseRunApp {

    private static final Logger log= LoggerFactory.getLogger(ProcessFunctionKeyedStream.class);

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
        FlinkKafkaConsumer<String> kafkaConsumer
                = getKafkaConsumer(KafkaConfigPo.kvTopic1, new SimpleStringSchema());


        // 注册广播变量
        BroadcastStream<Tuple2<String, String>> broadcast = env
                .fromCollection(getBroadcastData())
                .broadcast(new MapStateDescriptor<>("RulesBroadcastState", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO));

        env
                .addSource(kafkaConsumer)
                .map(new RichMapSplit2KV())
                .connect(broadcast)
                .process(new BroadcastProcessFunction<Tuple2<String, Long>, Tuple2<String, String>, Tuple3<String, String, Long>>() {

                    private static final long serialVersionUID = 6212404377214953324L;
                    final MapStateDescriptor<String, String> ruleStateDescriptor = new MapStateDescriptor<>(
                            "RulesBroadcastState",
                            BasicTypeInfo.STRING_TYPE_INFO,
                            BasicTypeInfo.STRING_TYPE_INFO
                    );

                    @Override
                    public void processElement(Tuple2<String, Long> value, ReadOnlyContext ctx, Collector<Tuple3<String, String, Long>> out) throws Exception {
                        log.info("============================== processElement, value: {}==============================", value);
                        out.collect(new Tuple3<>(value.f0, ctx.getBroadcastState(ruleStateDescriptor).get(value.f0), value.f1));

                    }

                    @Override
                    public void processBroadcastElement(Tuple2<String, String> value, Context ctx, Collector<Tuple3<String, String, Long>> out) throws Exception {
                        log.info("============================== processBroadcastElement, value: {}==============================", value);
                        ctx.getBroadcastState(ruleStateDescriptor).put(value.f0, value.f1);
                    }
                })
                .print("ProcessFunctionNoKeyedBroadcast result:");
        env.execute(JobConfigPo.jobNamePrefix + ProcessFunctionNoKeyedBroadcast.class.getName());

    }


}
