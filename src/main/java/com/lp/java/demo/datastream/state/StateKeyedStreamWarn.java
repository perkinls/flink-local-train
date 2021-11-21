package com.lp.java.demo.datastream.state;

import com.lp.java.demo.datastream.BaseStreamingEnv;
import com.lp.java.demo.datastream.IBaseRunApp;
import com.lp.java.demo.commons.po.SensorPo;
import com.lp.java.demo.commons.po.config.JobConfigPo;
import com.lp.java.demo.commons.po.config.KafkaConfigPo;
import com.lp.java.demo.datastream.richfunction.RichMapSplit2Sensor;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author li.pan
 * @version 1.0.0
 * @title KeyedProcessFunction 用于KeyedStream，keyBy之后的流处理
 * @createTime 2021年03月08日 13:36:00
 * 需求:
 * 同一传感器两次温度相差10度触发报警
 */
public class StateKeyedStreamWarn extends BaseStreamingEnv<String> implements IBaseRunApp {

    private final static Logger log = LoggerFactory.getLogger(StateKeyedStreamWarn.class);

    @Override
    public void doMain() throws Exception {
        FlinkKafkaConsumer<String> kafkaConsumer
                = getKafkaConsumer(KafkaConfigPo.sensorTopic, new SimpleStringSchema());

        env
                .addSource(kafkaConsumer)
                .map(new RichMapSplit2Sensor())
                .keyBy((KeySelector<SensorPo, String>) SensorPo::getId)
                // .process(new sensorTempDiff10())
                .process(new sensor10sTempContinueRise(5))
                .print();

        env.execute(JobConfigPo.jobNamePrefix + StateKeyedStreamWarn.class.getName());

    }

    /**
     * 同一传感器两次温度相差10度触发报警
     */
    private static class sensorTempDiff10 extends KeyedProcessFunction<String, SensorPo, Object> {

        private static final long serialVersionUID = 6489263741522191430L;
        private ValueState<Double> lastTemp;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTemp = getRuntimeContext().getState(
                    // 对于状态的描述,Flink底层根据相应的状态去赵对应的状态
                    new ValueStateDescriptor<>("sensor-last-temp", TypeInformation.of(Double.class))
            );
        }

        @Override
        public void processElement(SensorPo value, Context ctx, Collector<Object> out) throws Exception {
            if (lastTemp.value() == null) { // 状态为空,未给定 默认值 的情况下
                lastTemp.update(0.0);
            }
            Double prevTemp = lastTemp.value();

            log.info("Last temperature is: ({}),Current temperature is: ({})", prevTemp, value.getTemperature());
            if (prevTemp == 0.0 || value.getTemperature() < prevTemp - 10) {
                lastTemp.update(value.getTemperature());
            } else if (value.getTemperature() > prevTemp + 10) {
                lastTemp.update(value.getTemperature());
                out.collect("传感器" + ctx.getCurrentKey() + "两次温度相差超过10℃");
            } else {
                lastTemp.update(value.getTemperature());
            }
        }
    }

    /**
     * xx秒内传感器温度持续上升
     */
    private static class sensor10sTempContinueRise extends KeyedProcessFunction<String, SensorPo, Object> {
        private static final long serialVersionUID = 8480378976928118465L;
        //时间间隔
        private Integer interval;

        // 记录上一次温度值状态
        private transient ValueState<Double> lastTempState;
        // 记录Timer需要触发的初始时间戳
        private transient ValueState<Long> timerTsState;

        public sensor10sTempContinueRise(Integer interval) {
            this.interval = interval;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            //上一次的温度的state描述器
            ValueStateDescriptor<Double> lastTempStateDescriptor = new ValueStateDescriptor<>("sensor-last-temp", Double.class);
            //获取state
            lastTempState = getRuntimeContext().getState(lastTempStateDescriptor);

            //定时器结束时间的state描述器
            ValueStateDescriptor<Long> timerTsStateDescriptor = new ValueStateDescriptor<Long>("sensor-timer-ts", Long.class);
            timerTsState = getRuntimeContext().getState(timerTsStateDescriptor);
        }

        @Override
        public void processElement(SensorPo value, Context ctx, Collector<Object> out) throws Exception {

            // 初始化为null，给定默认值
            if (lastTempState.value() == null) {
                lastTempState.update(0.0);
            }

            if (timerTsState.value() == null) {
                timerTsState.update(0L);
            }

            Double lastTemp = lastTempState.value();
            Long timerTs = timerTsState.value();

            //如果温度上升，注册10秒定时器，开始等待
            if (value.getTemperature() > lastTemp && timerTs == 0) {
                //计算定时器触发的时间戳
                Long ts = ctx.timerService().currentProcessingTime() + interval * 1000L;
                System.out.println("ts = " + ts);
                log.info("定时器待触发的timestamp={}", ts);
                //在处理时间ProcessingTime上，设置了定时器
                ctx.timerService().registerProcessingTimeTimer(ts);
                timerTsState.update(ts);
            } else if (value.getTemperature() < lastTemp) {

                log.info("{}s内,温度有下降.删除用于回调的定时器", interval);
                //温度减低了，删掉定时器
                if (timerTs != null) {
                    ctx.timerService().deleteProcessingTimeTimer(timerTs);
                }
                timerTsState.clear();
            }

            //更新最近一次温度的state
            lastTempState.update(value.getTemperature());
        }


        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Object> out) throws Exception {
            //定时器触发，输出报警信息
            out.collect(interval + "s内传感器" + ctx.getCurrentKey().toString() + "温度值连续上升");
            timerTsState.clear();

            System.out.println("timerTsState.clear()");
        }

        @Override
        public void close() throws Exception {
            lastTempState.clear();
        }
    }

}
