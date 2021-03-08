package com.lp.java.demo.datastream.process;

import com.lp.java.demo.commons.BaseStreamingEnv;
import com.lp.java.demo.commons.IBaseRunApp;
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
public class ProcessFunctionKeyedStream extends BaseStreamingEnv<String> implements IBaseRunApp {

    private final static Logger log = LoggerFactory.getLogger(ProcessFunctionKeyedStream.class);

    @Override
    public void doMain() throws Exception {
        FlinkKafkaConsumer<String> kafkaConsumer
                = getKafkaConsumer(KafkaConfigPo.sensorTopic, new SimpleStringSchema());

        env
                .addSource(kafkaConsumer)
                .map(new RichMapSplit2Sensor())
                .keyBy((KeySelector<SensorPo, String>) SensorPo::getId)
                .process(new sensorTempDiff10())
                .print();

        env.execute(JobConfigPo.jobNamePrefix + ProcessFunctionKeyedStream.class.getName());

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
            if (lastTemp.value() == null) { // 状态为空 未给定 默认值 的情况下
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


}
