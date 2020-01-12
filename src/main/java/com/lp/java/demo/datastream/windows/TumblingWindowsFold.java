package com.lp.java.demo.datastream.windows;

import com.lp.scala.demo.utils.ConfigUtils;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import com.lp.java.demo.datastream.trigger.CustomProcessingTimeTrigger;
import com.lp.java.demo.datastream.util.Split2KV;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;

import java.util.Properties;

/**
 * <p/>
 * <li>Description: 滚动窗口的FoldFunction聚合函数</li>
 * FoldFunction指定窗口的输入数据元如何与输出类型的数据元组合
 * <li>@author: panli0226@sina.com</li>
 * <li>Date: 2020-01-07 20:34</li>
 */
public class TumblingWindowsFold {
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置最少一次处理语义和恰一次处理语义
//        env.enableCheckpointing(20000,CheckpointingMode.EXACTLY_ONCE);
//		checkpoint 也可以分开设置
//        env.enableCheckpointing(20000);
//		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);

//		设置checkpoint目录
//		env.setStateBackend(new FsStateBackend("/hdfs/checkpoint"));
//        env.getCheckpointConfig() // checkpoint的清楚策略
//                .enableExternalizedCheckpoints(CheckpointConfig.
//                        ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        /**
         * 设置重启策略/5次尝试/每次尝试间隔50s
         */
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 50000));


        // 选择设置时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        scala.Tuple2<String, Properties> kafkaConfig = ConfigUtils.apply("kv");
        FlinkKafkaConsumerBase kafkaConsumer =
                new FlinkKafkaConsumer(kafkaConfig._1, new SimpleStringSchema(), kafkaConfig._2)
                        .setStartFromLatest();

        SingleOutputStreamOperator<String> fold = env
                .addSource(kafkaConsumer)
                .map(new Split2KV())
                .keyBy(0)
//                .windowAll(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(10)))
                .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(10)))
                .trigger(CustomProcessingTimeTrigger.create())
                .fold("", new FoldFunction<Tuple2<String, Long>, String>() {

                    // 字符串拼接功能，初始字符串为空，然后将数字追加其后
                    @Override
                    public String fold(String acc, Tuple2<String, Long> value) throws Exception {
                        return acc + value.f1;
                    }
                });

        fold.print();

        env.execute(TumblingWindowsFold.class.getCanonicalName());
    }
}
