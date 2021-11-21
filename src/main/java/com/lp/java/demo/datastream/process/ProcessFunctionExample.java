package com.lp.java.demo.datastream.process;

import com.lp.java.demo.commons.po.config.KafkaConfigPo;
import com.lp.java.demo.datastream.BaseStreamingEnv;
import com.lp.java.demo.datastream.IBaseRunApp;
import com.lp.java.demo.datastream.richfunction.RichMapSplit2KV;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author li.pan
 * @title ProcessFunction简单案例
 * @Date 2021/11/21
 * <p>
 * 以下示例维护了每个key的求和，并且每一分钟发出一个没有更新key的key/count对。
 * 　　1、count，key和last-modification-timestamp保存在ValueState中，该ValueState由key隐含地限定。
 * 　　2、对于每条记录，ProcessFunction增加counter的求和并设置最后更新时间戳
 * 　　3、该函数还会在未来一分钟内安排回调(基于event time)
 * 　　4、在每次回调时，它会根据存储的count的最后更新时间来检查callback的事件时间，如果匹配的话，就会将key/count对发出来
 */
public class ProcessFunctionExample extends BaseStreamingEnv<String> implements IBaseRunApp {
    private static final Logger log = LoggerFactory.getLogger(ProcessFunctionExample.class);


    @Override
    protected void setKafkaFromOffsets(FlinkKafkaConsumer<String> consumer) {
        consumer.setStartFromLatest();
    }


    @Override
    public void doMain() throws Exception {
        FlinkKafkaConsumer<String> kafkaConsumer
                = getKafkaConsumer(KafkaConfigPo.kvTopic1, new SimpleStringSchema());

        SingleOutputStreamOperator<Tuple2<String, Long>> res = env
                .addSource(kafkaConsumer)
                .map(new RichMapSplit2KV())
                .keyBy((KeySelector<Tuple2<String, Long>, String>) value -> value.f0)
                .process(new CountWithTimeoutFunction());

        res.print("ProcessFunctionExample result");

        env.execute();
    }


    /**
     * ProcessFunction的实现，用来维护计数和超时
     */
    static class CountWithTimeoutFunction extends ProcessFunction<Tuple2<String, Long>, Tuple2<String, Long>> {

        private static final long serialVersionUID = -2363137157207838914L;
        /**
         * process function维持的状态
         */
        private ValueState<CountWithTimestamp> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
        }

        @Override
        public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Tuple2<String, Long>> out)
                throws Exception {
            log.info("======================== 【processElement】 ========================");
            // 获取当前的count
            CountWithTimestamp current = state.value();
            if (current == null) {
                current = new CountWithTimestamp();
                current.key = value.f0;
            }

            // 更新 state 的 count
            current.count = current.count + value.f1;

            // 将state的时间戳设置为记录的分配事件时间戳
            current.lastModified = ctx.timestamp();

            // 将状态写回
            state.update(current);
            log.info("当前WaterMark: {}",ctx.timerService().currentWatermark());
            log.info("当前处理时间: {}",ctx.timerService().currentProcessingTime());

            // 从当前事件时间开始计划下一个5秒的定时器
            //  1.12默认事件时间？ 因为当前事件时间不存在,注册EventTime不会生效
            ctx.timerService().registerProcessingTimeTimer(current.lastModified + 5000);

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out)
                throws Exception {

            log.info("======================== 【onTimer】 ========================");

            //获取计划定时器的key的状态
            CountWithTimestamp result = state.value();

            // 检查是否是过时的定时器或最新的定时器
            log.info("触发触发器,当前时间戳: {}, 状态时间戳: {}", timestamp, result.lastModified);
            if (timestamp == result.lastModified + 5000) {
                // emit the state on timeout
                out.collect(new Tuple2<>(result.key, result.count));
            }
        }
    }

    /**
     * state中保存的数据类型
     */
    static class CountWithTimestamp {

        public String key;
        public long count;
        public long lastModified;
    }
}
