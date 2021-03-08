package com.lp.java.demo.datastream.windows;

import com.lp.java.demo.commons.BaseStreamingEnv;
import com.lp.java.demo.commons.IBaseRunApp;
import com.lp.java.demo.commons.po.config.JobConfigPo;
import com.lp.java.demo.commons.po.config.KafkaConfigPo;
import com.lp.java.demo.datastream.processfunction.util.Split2KV;
import com.lp.java.demo.datastream.windows.trigger.CustomProcessingTimeTrigger;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * <p/>
 * <li>Description: 滚动窗口的ReductionFunction聚合函数</li>
 * <li>@author: panli0226@sina.com</li>
 * <li>Date: 2020-01-07 20:34</li>
 */
public class TumblingWindowsReduceFunction extends BaseStreamingEnv<String> implements IBaseRunApp {

    @Override
    public void doMain() throws Exception {

        FlinkKafkaConsumer<String> kafkaConsumer =
                getKafkaConsumer(KafkaConfigPo.kvTopic1, new SimpleStringSchema());

        SingleOutputStreamOperator<Tuple2<String, Long>> fold = env
                .addSource(kafkaConsumer)
                .map(new Split2KV())
                .keyBy((KeySelector<Tuple2<String, Long>, String>) value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .trigger(CustomProcessingTimeTrigger.create())
                .reduce((ReduceFunction<Tuple2<String, Long>>) (v1, v2) -> new Tuple2<>(v1.f0, v1.f1 + v2.f1));

        fold.print();

        env.execute(JobConfigPo.jobNamePrefix + TumblingWindowsReduceFunction.class.getName());

    }
}
