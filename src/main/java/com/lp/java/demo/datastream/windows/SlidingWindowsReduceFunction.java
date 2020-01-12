package com.lp.java.demo.datastream.windows;

import com.lp.java.demo.datastream.util.Split2KV;
import com.lp.scala.demo.utils.ConfigUtils;
import org.apache.flink.api.common.functions.ReduceFunction;
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
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;

import java.util.Properties;

/**
 * <p/>
 * <li>Description: 滑动窗口内聚合</li>
 * 以第一个元素作为基准，窗口大小为10和滑动都为10
 * <li>@author: panli0226@sina.com</li>
 * <li>Date: 2020-01-07 20:36</li>
 */
public class SlidingWindowsReduceFunction {
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置最少一次处理语义和恰一次处理语义
//       env.enableCheckpointing(20000,CheckpointingMode.EXACTLY_ONCE);
//		checkpoint 也可以分开设置
//      env.enableCheckpointing(20000);
//		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);

//		设置checkpoint目录
//		env.setStateBackend(new FsStateBackend("/hdfs/checkpoint"));

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


        SingleOutputStreamOperator<Tuple2<String, Long>> reduce = env
                .addSource(kafkaConsumer)
                .map(new Split2KV())
//                .windowAll(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(10)))
                .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(10)))
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> v1, Tuple2<String, Long> v2) throws Exception {
                        return new Tuple2<>(v1.f0, v1.f1 + v2.f1);
                    }
                });

        reduce.print();

        env.execute(SlidingWindowsReduceFunction.class.getCanonicalName());
    }
}
