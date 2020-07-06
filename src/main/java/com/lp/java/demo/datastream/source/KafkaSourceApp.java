package com.lp.java.demo.datastream.source;

import com.lp.java.demo.datastream.trigger.CustomProcessingTimeTrigger;
import com.lp.scala.demo.utils.ConfigUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import scala.Tuple2;

import java.util.Properties;

/**
 * <p/>
 * <li>Description: kafka消费者测试,自定义触发器</li>
 * <li>@author: panli0226@sina.com</li>
 * <li>Date: 2019-05-07 22:31</li>
 */
public class KafkaSourceApp {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置最少一次处理语义和恰一次处理语义
//		env.enableCheckpointing(20000,CheckpointingMode.EXACTLY_ONCE);
//		checkpoint 也可以分开设置
//		env.enableCheckpointing(20000);
//		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
//		设置checkpoint目录
//		env.setStateBackend(new FsStateBackend("/hdfs/checkpoint"));
//
//        // checkpoint的清楚策略
//        env.getCheckpointConfig()
//                .enableExternalizedCheckpoints(CheckpointConfig.
//                        ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        /**
         * 设置重启策略/5次尝试/每次尝试间隔50s
         */
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 50000));


        // 选择设置事件事件和处理事件
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        Tuple2<String, Properties> kafkaConfig = ConfigUtils.apply("string");
        FlinkKafkaConsumerBase kafkaConsumer =
                new FlinkKafkaConsumer(kafkaConfig._1, new SimpleStringSchema(), kafkaConfig._2)
                .setStartFromLatest();

        AllWindowedStream<Integer, TimeWindow> stream =
                env
                .addSource(kafkaConsumer)
                .map(new String2Integer())
                .timeWindowAll(Time.seconds(20))
                .trigger(CustomProcessingTimeTrigger.create());

        stream.sum(0).print();

        // 获取JobGraph
        Iterable<JobVertex> vertices = env.getStreamGraph().getJobGraph().getVertices();
        for (JobVertex vertex : vertices) {
            System.out.println("=====>" + vertex.getName());
            System.out.println("=====>" + vertex.getID());
        }

        env.execute(KafkaSourceApp.class.getCanonicalName());
    }

    private static class String2Integer extends RichMapFunction<String, Integer> {
        private static final long serialVersionUID = 1180234853172462378L;

        @Override
        public Integer map(String event) throws Exception {

            return Integer.valueOf(event);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
        }
    }

}
