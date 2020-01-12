package com.lp.java.demo.datastream.windows;

import com.lp.java.demo.datastream.trigger.CustomProcessingTimeTrigger;
import com.lp.java.demo.datastream.util.Split2KV;
import com.lp.scala.demo.utils.ConfigUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * <p/>
 * <li>Description: 统计一个window中元素个数，此外，还将window的信息添加到输出中。</li>
 * 使用ProcessWindowFunction来做简单的聚合操作，如:计数操作，性能是相当差的。
 * 将ReduceFunction跟ProcessWindowFunction结合起来，来获取增量聚合和添加到ProcessWindowFunction中的信息，性能更好
 * <li>@author: panli0226@sina.com</li>
 * <li>Date: 2020-01-07 22:35</li>
 */
public class TumblingWindowsProcessFunction {
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置最少一次处理语义和恰一次处理语义
//        env.enableCheckpointing(20000,CheckpointingMode.EXACTLY_ONCE);
//		checkpoint 也可以分开设置
//        env.enableCheckpointing(20000);
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


        SingleOutputStreamOperator<String> process =
                env
                .addSource(kafkaConsumer)
                .map(new Split2KV())
                .keyBy(0)
                .timeWindow(Time.minutes(5))
                .trigger(CustomProcessingTimeTrigger.create())
                .process(new MyProcessWindowFunction());


        process.print();
        env.execute(TumblingWindowsProcessFunction.class.getCanonicalName());
    }



    /**
     * <p>
     * ProcessWindowFunction一次性迭代整个窗口里的所有元素，比较重要的一个对象是Context，可以获取到事件和状态信息
     * ，这样我们就可以实现更加灵活的控制，这实际上是process的主要特点吧。
     * 该算子会浪费很多性能吧，主要原因是不是增量计算，要缓存整个窗口然后再去处理，所以要设计好内存占比。
     * 当然了processWindowFunction可以结合 ReduceFunction, an AggregateFunction, or a FoldFunction来做增量计算。
     * </p>
     * 注意key类型 Tuple1
     */
    private static class MyProcessWindowFunction extends ProcessWindowFunction<Tuple2<String, Long>, String, Tuple1, TimeWindow> {

        @Override
        public void process(Tuple1 key, Context context, Iterable<Tuple2<String, Long>> input, Collector<String> out) {
            long count = 0;
            for (Tuple2<String, Long> in : input) {
                count++;
            }
            System.out.println("--------"+count);
            out.collect("Window: " + context.window() + "count: " + count);
        }
    }
}
