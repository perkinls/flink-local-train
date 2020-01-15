package com.lp.java.demo.datastream.join;

import com.lp.java.demo.datastream.util.Split2KV;
import com.lp.scala.demo.utils.ConfigUtils;
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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;

import java.util.Properties;

/**
 * <p/>
 * <li>title: 双流滑动窗口join</li>
 * <li>@author: li.pan</li>
 * <li>Date: 2020/1/15 1:08 下午</li>
 * <li>Version: V1.0</li>
 * <li>Description: 滑动窗口join</li>
 */
public class SlidingWindowJoin {
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
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        scala.Tuple2<String, Properties> kv1 = ConfigUtils.apply("kv");
        scala.Tuple2<String, Properties> kv2 = ConfigUtils.apply("kv1");
        /**
         * 从最新的offset开始消费消息
         * 设置自定义时间戳分配器和watermark发射器，也可以在后面的算子中设置
         */
        FlinkKafkaConsumerBase kafkaConsumerKv1 =
                new FlinkKafkaConsumer(kv1._1, new SimpleStringSchema(), kv1._2)
                        .setStartFromLatest();
        FlinkKafkaConsumerBase kafkaConsumerKv2 =
                new FlinkKafkaConsumer(kv2._1, new SimpleStringSchema(), kv2._2)
                        .setStartFromLatest();

        SingleOutputStreamOperator<Tuple2<String, Long>> operator = env
                .addSource(kafkaConsumerKv1)
                .map(new Split2KV());

        SingleOutputStreamOperator<Tuple2<String, Long>> operator1 = env
                .addSource(kafkaConsumerKv2)
                .map(new Split2KV());
        operator
                .join(operator1)
                .where(new KeySelector<Tuple2<String, Long>, String>() {

                    @Override
                    public String getKey(Tuple2<String, Long> value) throws Exception {
                        return value.f0;
                    }
                })
                .equalTo(new KeySelector<Tuple2<String, Long>, String>() {

                    @Override
                    public String getKey(Tuple2<String, Long> value) throws Exception {
                        return value.f0;
                    }
                })
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(10))) // (滚动窗口)
                //使用执行的用户函数完成连接操作
                .apply(new JoinFunction<Tuple2<String, Long>, Tuple2<String, Long>, Tuple3<String, Long, Long>>() {

                    @Override
                    public Tuple3<String, Long, Long> join(Tuple2<String, Long> first, Tuple2<String, Long> second) throws Exception {
                        Tuple3<String, Long, Long> tuple3 = new Tuple3<>();
                        tuple3.setField(first.f0, 0);
                        tuple3.setField(first.f1, 1);
                        tuple3.setField(second.f1, 2);
                        return tuple3;
                    }
                })
                .keyBy(0)
                .reduce(new ReduceFunction<Tuple3<String, Long, Long>>() {
                    @Override
                    public Tuple3<String, Long, Long> reduce(Tuple3<String, Long, Long> v1, Tuple3<String, Long, Long> v2) {
                        Tuple3<String, Long, Long> tuple3 = new Tuple3<>();
                        tuple3.setField(v1.f0, 0);
                        tuple3.setField(v1.f1 + v2.f1, 1);
                        tuple3.setField(v1.f2 + v2.f2, 2);
                        return tuple3;
                    }
                }).print();

        env.execute(TumblingWindowJoin.class.getCanonicalName());
    }
}
