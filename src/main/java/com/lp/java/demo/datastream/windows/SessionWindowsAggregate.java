package com.lp.java.demo.datastream.windows;

import com.lp.java.demo.datastream.trigger.CustomProcessingTimeTrigger;
import com.lp.java.demo.datastream.util.Split2KV;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

public class SessionWindowsAggregate {
    private static class AverageAggregate implements
            AggregateFunction<Tuple2<String, Long>, Tuple2<Long, Long>, Double> {

        // Tuple2 第一个元素用来累加，第二个用来计数的
        @Override
        public Tuple2<Long, Long> createAccumulator() {
            return new Tuple2<>(0L, 0L);
        }


        @Override
        public Tuple2<Long, Long> add(Tuple2<String, Long> value, Tuple2<Long, Long> accumulator) {
            return new Tuple2<>(accumulator.f0 + value.f1, accumulator.f1 + 1L);
        }

        /**
         * 从累加器中获取结果
         *
         * @param accumulator
         * @return
         */
        @Override
        public Double getResult(Tuple2<Long, Long> accumulator) {
            return ((double) accumulator.f0) / accumulator.f1;
        }

        /**
         * 合并两个累加器，返回一个新的累加器
         *
         * @param a
         * @param b
         * @return
         */
        @Override
        public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
            return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
        }
    }

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置最少一次处理语义和恰一次处理语义
        env.enableCheckpointing(20000, CheckpointingMode.EXACTLY_ONCE);
//		checkpoint 也可以分开设置
        env.enableCheckpointing(20000);
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
        properties.setProperty("group.id", "SessionWindowsAggregate");

        FlinkKafkaConsumer010<String> kafkaConsumer010 = new FlinkKafkaConsumer010<>("KV",
                new SimpleStringSchema(),
                properties);

        // 设置从最新的开始消费
        kafkaConsumer010.setStartFromLatest();

        SingleOutputStreamOperator<Double> aggregate = env
                .addSource(kafkaConsumer010)
                .map(new Split2KV())
                .keyBy(0) // 对于tuble类的可以直接使用下标，否则只能自定义keyselector
//                .windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
//                .windowAll(EventTimeSessionWindows.withGap(Time.seconds(10)))
                .windowAll(ProcessingTimeSessionWindows.withDynamicGap((element) -> {
                    // 可以用事件里的标志自定义间隔，注意是浩渺级别的，我们这里是10s
                    return 10000;
                }))
                .trigger(CustomProcessingTimeTrigger.create()) //用了自定义触发器
                .aggregate(new AverageAggregate());//使用聚合函数

        aggregate.print();

        env.execute(SessionWindowsAggregate.class.getCanonicalName());
    }

}
