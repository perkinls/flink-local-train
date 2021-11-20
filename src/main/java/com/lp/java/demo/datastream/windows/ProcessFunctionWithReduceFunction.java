package com.lp.java.demo.datastream.windows;

import com.lp.java.demo.datastream.BaseStreamingEnv;
import com.lp.java.demo.datastream.IBaseRunApp;
import com.lp.java.demo.commons.po.config.JobConfigPo;
import com.lp.java.demo.commons.po.config.KafkaConfigPo;
import com.lp.java.demo.datastream.richfunction.RichMapSplit2KV;
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
import org.apache.flink.table.shaded.org.joda.time.DateTime;
import org.apache.flink.table.shaded.org.joda.time.DateTimeZone;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author li.pan
 * @version 1.0.0
 * @title 使用ReduceFunction与ProcessWindowFunction增量聚合及获取元数据信息
 * @createTime 2021年03月08日 12:43:00
 */
public class ProcessFunctionWithReduceFunction extends BaseStreamingEnv<String> implements IBaseRunApp {

    private static final Logger log = LoggerFactory.getLogger(ProcessFunctionTumblingWindows.class);

    @Override
    protected Integer setDefaultParallelism() {
        return 1;
    }

    @Override
    protected void setKafkaFromOffsets(FlinkKafkaConsumer<String> consumer) {
        consumer.setStartFromLatest();
    }

    @Override
    public void doMain() throws Exception {
        // 获取Kafka消费者
        FlinkKafkaConsumer<String> kafkaConsumer =
                getKafkaConsumer(KafkaConfigPo.kvTopic1, new SimpleStringSchema());


        // 转换和窗口计算
        SingleOutputStreamOperator<Tuple3<Tuple2<String, Long>, String, String>> reduce = env
                .addSource(kafkaConsumer)
                .map(new RichMapSplit2KV())
                .keyBy((KeySelector<Tuple2<String, Long>, String>) value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10))) // 指定为process的滚动窗口
                .reduce(new MyReduceFunction(), new MyProcessWindowFunction()).setParallelism(4);

        reduce.print("ProcessFunctionWithReduceFunction result");
        env.execute(JobConfigPo.jobNamePrefix + ProcessFunctionWithReduceFunction.class.getName());
    }

    /**
     * 增量聚合
     */
    private static class MyReduceFunction implements ReduceFunction<Tuple2<String, Long>> {

        private static final long serialVersionUID = -1360032652256634248L;

        // tips: 暂时从输出结果只能观察到对于一个相同的key的增量累加(来一个基于之前的状态累加)
        // TODO 暂时不能观察多个key的调用
        @Override
        public Tuple2<String, Long> reduce(Tuple2<String, Long> v1, Tuple2<String, Long> v2) throws Exception {
            log.info("---------------- reduce ----------------ele1: {}, ele2: {}", v1, v2);
            return new Tuple2<>(v1.f0, v1.f1 + v2.f1); // 相同key元素累加
        }
    }

    /**
     * processWindow 可以获取窗口的元数据信息
     */
    private static class MyProcessWindowFunction extends ProcessWindowFunction<Tuple2<String, Long>, Tuple3<Tuple2<String, Long>,  String, String>, String, TimeWindow> {

        private static final long serialVersionUID = -6323616947395567966L;

        @Override
        public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<Tuple3<Tuple2<String, Long>, String, String>> out) throws Exception {
            // 计算结果 窗口开始结束时间
            Tuple2<String, Long> resTp = elements.iterator().next();
            log.info("+++++++++++++++++++++++ process +++++++++++++++++++++++");
            log.info("+++++++++++++++++++++++ {} +++++++++++++++++++++++", resTp);


            String windowStart=new DateTime(context.window().getStart(), DateTimeZone.forID("+08:00")).toString("yyyy-MM-dd HH:mm:ss");
            String windowEnd=new DateTime(context.window().getEnd(), DateTimeZone.forID("+08:00")).toString("yyyy-MM-dd HH:mm:ss");


            out.collect(new Tuple3<>(resTp, windowStart, windowEnd));
        }

    }
}
