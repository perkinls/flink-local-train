package com.lp.java.demo.datastream.windows;

import com.lp.java.demo.datastream.trigger.CustomProcessingTimeTrigger;
import com.lp.java.demo.datastream.util.Split2KV;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
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
        env.enableCheckpointing(20000,CheckpointingMode.EXACTLY_ONCE);
//		checkpoint 也可以分开设置
        env.enableCheckpointing(20000);
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
        properties.setProperty("group.id", "TumblingWindowsProcessFunction");

        FlinkKafkaConsumer010<String> kafkaConsumer010 = new FlinkKafkaConsumer010<>("KV",
                new SimpleStringSchema(),
                properties);

        kafkaConsumer010.setStartFromLatest();
        SingleOutputStreamOperator<String> process = env
                .addSource(kafkaConsumer010)
                .map(new Split2KV())
                .keyBy(value -> value.f0) //注意我这里的写法
                .timeWindow(Time.minutes(5)) //滚动窗口
                .trigger(CustomProcessingTimeTrigger.create())
                .process(new MyProcessWindowFunction());


        process.print();
        env.execute(TumblingWindowsProcessFunction.class.getCanonicalName());
    }

    /*
        ProcessWindowFunction一次性迭代整个窗口里的所有元素，比较重要的一个对象是Context，可以获取到事件和状态信息
        ，这样我们就可以实现更加灵活的控制，这实际上是process的主要特点吧。
        该算子会浪费很多性能吧，主要原因是不是增量计算，要缓存整个窗口然后再去处理，所以要设计好内存占比。

        当然了rocessWindowFunction可以结合 ReduceFunction, an AggregateFunction, or a FoldFunction来做增量计算。

     */
     private static  class MyProcessWindowFunction
            extends ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<Tuple2<String, Long>> input, Collector<String> out) {
            long count = 0;
            for (Tuple2<String, Long> in: input) {
                count++;
            }

            out.collect("Window: " + context.window() + "count: " + count);
        }
    }
}
