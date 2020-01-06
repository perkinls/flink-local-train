package com.lp.java.demo.datastream.join;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import com.lp.java.demo.datastream.util.Split2KV;

import javax.annotation.Nullable;
import java.util.Properties;

public class IntervalJoin {
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置最少一次处理语义和恰一次处理语义
        env.enableCheckpointing(20000,CheckpointingMode.EXACTLY_ONCE);
//		checkpoint 也可以分开设置
//        env.enableCheckpointing(20000);
//		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);

//		设置checkpoint目录
//		env.setStateBackend(new FsStateBackend("/hdfs/checkpoint"));
        env.getCheckpointConfig() // checkpoint的清楚策略
                .enableExternalizedCheckpoints(CheckpointConfig.
                        ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

//		设置重启策略
        env.setRestartStrategy(RestartStrategies.
                fixedDelayRestart(5,//5次尝试
                        50000)); //每次尝试间隔50s

        // 选择设置事件事件和处理事件
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "mt-mdh.local:9093");
        properties.setProperty("group.id", "SlidingWindowsReduceFunction");

        FlinkKafkaConsumer010<String> kafkaConsumer010 = new FlinkKafkaConsumer010<>("KV",
                new SimpleStringSchema(),
                properties);

        kafkaConsumer010.setStartFromLatest();

        FlinkKafkaConsumer010<String> kafkaConsumer010_1 = new FlinkKafkaConsumer010<>("KV1",
                new SimpleStringSchema(),
                properties);

        kafkaConsumer010_1.setStartFromLatest();

        SingleOutputStreamOperator<Tuple2<String, Long>> operator = env
                .addSource(kafkaConsumer010_1)
                .assignTimestampsAndWatermarks(new CustomWatermarkExtractor())
                .map(new Split2KV());

        SingleOutputStreamOperator<Tuple2<String, Long>> operator1 = env
                .addSource(kafkaConsumer010)
                .assignTimestampsAndWatermarks(new CustomWatermarkExtractor())
                .map(new Split2KV());

        operator.keyBy(0).intervalJoin(operator1.keyBy(0))
                .between(Time.milliseconds(-2), Time.milliseconds(1))
                .process(new ProcessJoinFunction<Tuple2<String, Long> , Tuple2<String, Long> , Tuple2<String, Long>>(){

                    @Override
                    public void processElement(Tuple2<String, Long> left, Tuple2<String, Long> right, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                        Tuple2<String, Long> tuple2 = new Tuple2<>();
                        tuple2.setField(left.f0,0);
                        tuple2.setField(left.f1+right.f1,1);
                        out.collect(tuple2 );
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
