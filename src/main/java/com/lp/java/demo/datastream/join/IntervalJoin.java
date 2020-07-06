package com.lp.java.demo.datastream.join;

import com.lp.java.demo.datastream.util.Split2KV;
import com.lp.scala.demo.utils.ConfigUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * <p/>
 * <li>title: 双流间隔join</li>
 * <li>@author: li.pan</li>
 * <li>Date: 2020/1/15 1:08 下午</li>
 * <li>Version: V1.0</li>
 * <li>Description: 间隔join</li>
 * 测试的时候将kafkaProduce中类kv数据生成方法 join情况打开
 */
public class IntervalJoin {
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置最少一次处理语义和恰一次处理语义
        env.enableCheckpointing(20000, CheckpointingMode.EXACTLY_ONCE);
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
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        scala.Tuple2<String, Properties> kv1 = ConfigUtils.apply("kv");
        scala.Tuple2<String, Properties> kv2 = ConfigUtils.apply("kv1");
        /**
         * 从最新的offset开始消费消息
         * 设置自定义时间戳分配器和watermark发射器，也可以在后面的算子中设置
         */
        FlinkKafkaConsumerBase kafkaConsumerKv1 =
                new FlinkKafkaConsumer(kv1._1, new SimpleStringSchema(), kv1._2)
                        .assignTimestampsAndWatermarks(new CustomWatermarkExtractor())
                        .setStartFromLatest();
        FlinkKafkaConsumerBase kafkaConsumerKv2 =
                new FlinkKafkaConsumer(kv2._1, new SimpleStringSchema(), kv2._2)
                        .assignTimestampsAndWatermarks(new CustomWatermarkExtractor())
                        .setStartFromLatest();


        SingleOutputStreamOperator<Tuple2<String, Long>> operator = env
                .addSource(kafkaConsumerKv1)
                .map(new Split2KV());

        SingleOutputStreamOperator<Tuple2<String, Long>> operator1 = env
                .addSource(kafkaConsumerKv2)
                .map(new Split2KV());

        operator
                .keyBy(0)
                .intervalJoin(operator1.keyBy(0))
                .between(Time.milliseconds(-2), Time.milliseconds(1))
                // 执行join后函数
                .process(new ProcessJoinFunction<Tuple2<String, Long>, Tuple2<String, Long>, Tuple2<String, Long>>() {

                    @Override
                    public void processElement(Tuple2<String, Long> left, Tuple2<String, Long> right, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                        Tuple2<String, Long> tuple2 = new Tuple2<>();
                        tuple2.setField(left.f0, 0);
                        tuple2.setField(left.f1 + right.f1, 1);
                        out.collect(tuple2);
                    }
                })
                .print();
        env.execute(IntervalJoin.class.getCanonicalName());
    }

    private static class CustomWatermarkExtractor implements AssignerWithPeriodicWatermarks<String> {

        private static final long serialVersionUID = -742759155861320823L;

        private long currentTimestamp = Long.MIN_VALUE;

        /**
         * @param event
         * @param previousElementTimestamp
         * @return
         */
        @Override
        public long extractTimestamp(String event, long previousElementTimestamp) {
            this.currentTimestamp = System.currentTimeMillis();
            return this.currentTimestamp;
        }

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - 1);
        }
    }
}
