package com.lp.java.demo.datastream.processfunction;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import javax.annotation.Nullable;
import java.util.Properties;

/*
    测试方法：
    先开启producer 跑一会儿，然后停掉producer，过一分钟以后启动，因为我们采用的时间是注入时间，所以过一分钟之后发数据
    事件时间大于1分钟，结果才会输出。

    也可以设计某一个key，每隔1.5分钟发一次，这样就可以看到结果输出了

    实际上该例子就是实现了会话窗口的功能。
 */
public class ProcessFunctionTimeout {
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


//		设置重启策略
        env.setRestartStrategy(RestartStrategies.
                fixedDelayRestart(5,//5次尝试
                        50000)); //每次尝试间隔50s

        // 选择设置事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "mt-mdh.local:9093");
        properties.setProperty("group.id", "ProcessFunctionTimeout");

        FlinkKafkaConsumer010<String> kafkaConsumer010 = new FlinkKafkaConsumer010<>("KV",
                new SimpleStringSchema(),
                properties);


        // 该自定义时间戳抽取器和触发器实际上是使用的注入时间，因为例子发的数据不带时间戳的。
        kafkaConsumer010.assignTimestampsAndWatermarks(new CustomWatermarkExtractor());

        // 设置从最新的开始消费
        kafkaConsumer010.setStartFromLatest();

        SingleOutputStreamOperator<Tuple2<String, Long>> process = env
                .addSource(kafkaConsumer010)
                .map(new Split2KV())
                .keyBy(0) // 对于tuble类的可以直接使用下标，否则只能自定义keyselector
                .process(new CountWithTimeoutFunction());
        process.print();

        env.execute(ProcessFunctionTimeout.class.getCanonicalName());
    }
    static public class Split2KV extends RichMapFunction<String, Tuple2<String,String>> {
        private static final long serialVersionUID = 1180234853172462378L;
        @Override
        public Tuple2<String,String> map(String event) throws Exception {
            String[] split = event.split(" ");
            return new Tuple2<>(split[0],split[1]);
        }
        @Override
        public void open(Configuration parameters) throws Exception {
        }
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
            currentTimestamp = System.currentTimeMillis();
            return currentTimestamp;
        }

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - 1);
        }
    }
}
