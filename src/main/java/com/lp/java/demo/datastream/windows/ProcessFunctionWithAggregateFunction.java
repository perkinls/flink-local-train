package com.lp.java.demo.datastream.windows;

import com.lp.java.demo.datastream.BaseStreamingEnv;
import com.lp.java.demo.datastream.IBaseRunApp;
import com.lp.java.demo.commons.po.config.JobConfigPo;
import com.lp.java.demo.commons.po.config.KafkaConfigPo;
import com.lp.java.demo.datastream.richfunction.RichMapSplit2KV;
import org.apache.flink.api.common.functions.AggregateFunction;
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
 * @title 使用AggregateFunction与ProcessWindowFunction增量聚合及获取元数据信息
 * @createTime 2021年03月08日 12:45:00
 */
public class ProcessFunctionWithAggregateFunction extends BaseStreamingEnv<String> implements IBaseRunApp {

    @Override
    public void doMain() throws Exception {
        FlinkKafkaConsumer<String> kafkaConsumer
                = getKafkaConsumer(KafkaConfigPo.kvTopic1, new SimpleStringSchema());

        SingleOutputStreamOperator<Tuple3<Double, Long, Long>> aggregate = env
                .addSource(kafkaConsumer)
                .map(new RichMapSplit2KV())
                .keyBy((KeySelector<Tuple2<String, Long>, String>) value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .aggregate(new AverageAggregate(), new MyProcessWindowFunction());

        aggregate.print();
        env.execute(JobConfigPo.jobNamePrefix + ProcessFunctionWithAggregateFunction.class.getName());

    }

    private static class AverageAggregate implements AggregateFunction<Tuple2<String, Long>, Tuple2<Long, Long>, Double> {

        private static final long serialVersionUID = -7748415078685671007L;

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
            System.out.println("触发: getResult 累加计算结果 \t" + accumulator.f0 + "---->" + accumulator.f1);
            return Double.valueOf(accumulator.f0);
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

    private static class MyProcessWindowFunction extends ProcessWindowFunction<Double, Tuple3<Double, Long, Long>, String, TimeWindow> {
        private static final long serialVersionUID = -6865760977964220845L;

        @Override
        public void process(String s, Context context, Iterable<Double> elements, Collector<Tuple3<Double, Long, Long>> out) throws Exception {

            out.collect(new Tuple3<>(elements.iterator().next(), context.window().getStart(), context.window().getEnd()));
        }
    }
}
