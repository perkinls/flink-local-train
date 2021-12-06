package com.lp.java.demo.datastream.windows;

import com.lp.java.demo.datastream.BaseStreamingEnv;
import com.lp.java.demo.base.IBaseRunApp;
import com.lp.java.demo.commons.po.config.JobConfigPo;
import com.lp.java.demo.commons.po.config.KafkaConfigPo;
import com.lp.java.demo.datastream.richfunction.RichMapSplit2KV;
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
    public Integer setDefaultParallelism() {
        return 1;
    }

    @Override
    protected void setKafkaFromOffsets(FlinkKafkaConsumer<String> consumer) {
        consumer.setStartFromLatest(); // 当前窗口聚合测试,从flink最新记录开始更容易观察
    }

    @Override
    public void doMain() throws Exception {

        // 获取kafka消费者（指定topic、序列化方式）
        FlinkKafkaConsumer<String> kafkaConsumer =
                getKafkaConsumer(KafkaConfigPo.kvTopic1, new SimpleStringSchema());

        /*
         * 添加数据源/数据转换/keyBy/滚动窗口/触发器/聚合
         *
         * tips:窗口未关闭时只会打印最后一个key聚合元素,窗口关闭时所有key都会打印
         */
        SingleOutputStreamOperator<Tuple2<String, Long>> fold = env
                .addSource(kafkaConsumer)
                .map(new RichMapSplit2KV())
                .keyBy((KeySelector<Tuple2<String, Long>, String>) value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
                .trigger(CustomProcessingTimeTrigger.create())
                .reduce((ReduceFunction<Tuple2<String, Long>>) (v1, v2) -> new Tuple2<>(v1.f0, v1.f1 + v2.f1));

        fold.print("TumblingWindow fold result:");

        env.execute(JobConfigPo.jobNamePrefix + TumblingWindowsReduceFunction.class.getName());

    }
}
