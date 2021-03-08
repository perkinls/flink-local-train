package com.lp.java.demo.datastream.windows;

import com.lp.java.demo.commons.BaseStreamingEnv;
import com.lp.java.demo.commons.IBaseRunApp;
import com.lp.java.demo.commons.po.config.JobConfigPo;
import com.lp.java.demo.commons.po.config.KafkaConfigPo;
import com.lp.java.demo.datastream.processfunction.util.Split2KV;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

/**
 * @author li.pan
 * @version 1.0.0
 * @title 使用ReduceFunction与ProcessWindowFunction增量聚合及获取元数据信息
 * @createTime 2021年03月08日 12:43:00
 */
public class ProcessFunctionWithReduceFunction extends BaseStreamingEnv<String> implements IBaseRunApp {
    @Override
    public void doMain() throws Exception {
        // 获取Kafka消费者
        FlinkKafkaConsumer<String> kafkaConsumer =
                getKafkaConsumer(KafkaConfigPo.kvTopic1, new SimpleStringSchema());

        // 转换和窗口计算
        SingleOutputStreamOperator<Tuple3<Tuple2<String, Long>,Long, Long>> reduce = env
                .addSource(kafkaConsumer)
                .map(new Split2KV())
                .keyBy((KeySelector<Tuple2<String, Long>, String>) value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .reduce(new MyReduceFunction(), new MyProcessWindowFunction());
        reduce.print();
        env.execute(JobConfigPo.jobNamePrefix + ProcessFunctionWithReduceFunction.class.getName());
    }

    private static class MyReduceFunction implements ReduceFunction<Tuple2<String, Long>> {
        private static final long serialVersionUID = -1360032652256634248L;

        @Override
        public Tuple2<String, Long> reduce(Tuple2<String, Long> v1, Tuple2<String, Long> v2) throws Exception {
            return new Tuple2<>(v1.f0, v1.f1 + v2.f1);
        }
    }

    private static class MyProcessWindowFunction extends ProcessWindowFunction<Tuple2<String, Long>, Tuple3<Tuple2<String, Long>,Long, Long>, String, TimeWindow> {

        private static final long serialVersionUID = -6323616947395567966L;


        @Override
        public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<Tuple3<Tuple2<String, Long>, Long, Long>> out) throws Exception {
            // 计算结果 窗口开始结束时间
            out.collect(new Tuple3<>(elements.iterator().next(),context.window().getStart(), context.window().getEnd()));
        }
    }
}
