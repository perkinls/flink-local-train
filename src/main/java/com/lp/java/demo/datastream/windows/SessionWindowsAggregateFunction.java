package com.lp.java.demo.datastream.windows;

import com.lp.java.demo.datastream.BaseStreamingEnv;
import com.lp.java.demo.datastream.IBaseRunApp;
import com.lp.java.demo.commons.po.config.JobConfigPo;
import com.lp.java.demo.commons.po.config.KafkaConfigPo;
import com.lp.java.demo.datastream.richfunction.RichMapSplit2KV;
import com.lp.java.demo.datastream.windows.trigger.CustomProcessingTimeTrigger;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p/>
 * <li>Description: 会话窗口具有动态间隙的处理时间进行</li>
 * <p>
 * 在会话窗口中按活动会话分配器组中的数据元。与翻滚窗口和滑动窗口相比，会话窗口不重叠并且没有固定的开始和结束时间。
 * 相反，当会话窗口在一段时间内没有接收到数据元时，即当发生不活动的间隙时，会关闭会话窗口。会话窗口分配器可以配置
 * 静态会话间隙或 会话间隙提取器函数，该函数定义不活动时间段的长度。当此期限到期时，当前会话将关闭，后续数据元将分配给新的会话窗口。
 * <p>
 * 测试时启动kafka生产者,一段时间后关闭kafka生产者
 * <li>@author: panli0226@sina.com</li>
 * <li>Date: 2020-01-07 21:35</li>
 */
public class SessionWindowsAggregateFunction extends BaseStreamingEnv<String> implements IBaseRunApp {

    private static final Logger log = LoggerFactory.getLogger(SessionWindowsAggregateFunction.class);


    @Override
    protected void setKafkaFromOffsets(FlinkKafkaConsumer<String> consumer) {
        consumer.setStartFromLatest();  // 当前窗口聚合测试,从flink最新记录开始更容易观察
    }

    @Override
    protected Integer setDefaultParallelism() {
        return 1;
    }

    @Override
    public void doMain() throws Exception {

        // 获取kafka消费者（指定topic、序列化方式）
        FlinkKafkaConsumer<String> kafkaConsumer =
                getKafkaConsumer(KafkaConfigPo.kvTopic1, new SimpleStringSchema());

        /*
         *  1. 对于tuple类的可以直接使用下标，否则只能自定义keyselector
         *  2. 可以用事件里的标志自定义间隔，注意是毫秒级别的，我们这里是10s
         *  3. 用了自定义触发器
         *  4. 使用聚合函数
         *
         * tips:测试结果,windowAll来一条就触发计算（window可以设置并行度，windowAll不能设置并行度）
         */
        SingleOutputStreamOperator<Double> aggregate = env
                .addSource(kafkaConsumer)
                .map(new RichMapSplit2KV())
                .windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
//                .windowAll(EventTimeSessionWindows.withGap(Time.seconds(10)))
//                .windowAll(ProcessingTimeSessionWindows.withDynamicGap((element) -> {
//                    return 10000;
//                }))
                .trigger(CustomProcessingTimeTrigger.create())
                .aggregate(new AverageAggregate());

        aggregate.print("SessionWindow reduce result");
        // 输出结果，session会话中累加

        env.execute(JobConfigPo.jobNamePrefix + SessionWindowsAggregateFunction.class.getName());
    }

    private static class AverageAggregate implements AggregateFunction<Tuple2<String, Long>, Tuple2<Long, Long>, Double> {

        private static final long serialVersionUID = -553441249695195791L;

        // Tuple2 第一个元素用来累加，第二个用来计数的
        @Override
        public Tuple2<Long, Long> createAccumulator() {
            log.debug("======================== createAccumulator ========================");
            return new Tuple2<>(0L, 0L);
        }


        @Override
        public Tuple2<Long, Long> add(Tuple2<String, Long> value, Tuple2<Long, Long> accumulator) {
            log.debug("======================== add ========================");

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
            log.debug("======================== getResult ========================");

            log.info("触发: getResult 累加计算结果 \t" + accumulator.f0 + "---->" + accumulator.f1);
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
            log.debug("======================== merge ========================");

            return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
        }
    }


}
