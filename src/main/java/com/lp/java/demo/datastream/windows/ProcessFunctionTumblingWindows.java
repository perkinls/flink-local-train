package com.lp.java.demo.datastream.windows;

import com.lp.java.demo.datastream.BaseStreamingEnv;
import com.lp.java.demo.datastream.IBaseRunApp;
import com.lp.java.demo.commons.po.config.JobConfigPo;
import com.lp.java.demo.commons.po.config.KafkaConfigPo;
import com.lp.java.demo.datastream.richfunction.RichMapSplit2KV;
import com.lp.java.demo.datastream.windows.trigger.CustomProcessingTimeTrigger;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
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
 * <p/>
 * <li>Description: 统计一个window中元素个数，此外，还将window的信息添加到输出中。</li>
 * 使用ProcessWindowFunction来做简单的聚合操作，如:计数操作，性能是相较差的。
 * <li>@author: panli0226@sina.com</li>
 * <li>Date: 2020-01-07 22:35</li>
 */
public class ProcessFunctionTumblingWindows extends BaseStreamingEnv<String> implements IBaseRunApp {
    private static final Logger log = LoggerFactory.getLogger(ProcessFunctionTumblingWindows.class);

    @Override
    protected Integer setDefaultParallelism() {
        return 1;
    }

    @Override
    protected void setKafkaFromOffsets(FlinkKafkaConsumer<String> consumer) {
        consumer.setStartFromLatest(); // 当前窗口测试,从flink最新记录开始更容易观察
    }

    @Override
    public void doMain() throws Exception {
        FlinkKafkaConsumer<String> kafkaConsumer =
                getKafkaConsumer(KafkaConfigPo.kvTopic1, new SimpleStringSchema());


        SingleOutputStreamOperator<String> process =
                env
                        .addSource(kafkaConsumer)
                        .map(new RichMapSplit2KV())
                        .keyBy((KeySelector<Tuple2<String, Long>, String>) value -> value.f0)
                        .window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
//                        .trigger(CustomProcessingTimeTrigger.create())
                        .process(new MyProcessWindowFunction());


        process.print("ProcessFunctionTumblingWindows result");
        env.execute(JobConfigPo.jobNamePrefix + ProcessFunctionTumblingWindows.class.getName());
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
    private static class MyProcessWindowFunction extends ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow> {

        private static final long serialVersionUID = -6879392760355936278L;
        long totalCount = 0;


        /**
         * process 函数是根据窗口内keyBy后key的值来触发的
         * 一个窗口结束的时候调用一次（一个分组执行一次），不适合大量数据，全量数据保存在内存中，会造成内存溢出
         *
         * @param s        key
         * @param context  上下文对象
         * @param elements 元素集合
         * @param out      输出
         * @throws Exception
         */
        @Override
        public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {

            log.info("+++++++++++++++++++++++ process +++++++++++++++++++++++");
            int count = 0;
            for (Tuple2<String, Long> in : elements) {
                count++;
            }
            log.info("当前窗口中key: {}, 元素总个数: {}, 当前key元素个数: {} ,元素: {}", s, totalCount = totalCount + count, count, elements.toString());
            String windowStart = new DateTime(context.window().getStart(), DateTimeZone.forID("+08:00")).toString("yyyy-MM-dd HH:mm:ss");
            String windowEnd = new DateTime(context.window().getEnd(), DateTimeZone.forID("+08:00")).toString("yyyy-MM-dd HH:mm:ss");

            log.info("窗口开始时间: {}", windowStart);
            log.info("窗口结束时间: {}", windowEnd);


            out.collect("Window: " + context.window() + "count: " + count);

        }
    }
}
