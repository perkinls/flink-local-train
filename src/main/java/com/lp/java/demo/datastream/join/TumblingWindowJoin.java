package com.lp.java.demo.datastream.join;

import com.lp.java.demo.datastream.util.Split2KV;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

public class TumblingWindowJoin {
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
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
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
                .map(new Split2KV());

        SingleOutputStreamOperator<Tuple2<String, Long>> operator1 = env
                .addSource(kafkaConsumer010)
                .map(new Split2KV());
        operator.join(operator1).where(new KeySelector<Tuple2<String, Long>, String>() {

            @Override
            public String getKey(Tuple2<String, Long> value) throws Exception {
                return value.f0;
            }
        }).equalTo(new KeySelector<Tuple2<String, Long>, String>() {

            @Override
            public String getKey(Tuple2<String, Long> value) throws Exception {
                return value.f0;
            }
        })
        .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(10))) // 滑动窗口
//        .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10))) // 会话窗口

//        .window(TumblingProcessingTimeWindows.of(Time.seconds(10))) // 滚动窗口
        .apply (new JoinFunction<Tuple2<String, Long>, Tuple2<String, Long>, Tuple3<String, Long,Long>>(){

            @Override
            public Tuple3<String, Long, Long> join(Tuple2<String, Long> first, Tuple2<String, Long> second) throws Exception {
                Tuple3<String, Long, Long> tuple3 = new Tuple3<>();
                tuple3.setField(first.f0,0);
                tuple3.setField(first.f1,1);
                tuple3.setField(second.f1,2);
                return tuple3;
            }
        }).keyBy(0)
        .reduce(new ReduceFunction<Tuple3<String, Long, Long>>() {
            public Tuple3<String, Long,Long> reduce(Tuple3<String, Long, Long> v1, Tuple3<String, Long, Long> v2) {
                Tuple3<String, Long,Long> tuple3 = new Tuple3<>();
                tuple3.setField(v1.f0,0);
                tuple3.setField(v1.f1+v2.f1,1);
                tuple3.setField(v1.f2+v2.f2,2);
                return tuple3;
            }
        }).print();

        env.execute(TumblingWindowJoin.class.getCanonicalName());
    }
}
