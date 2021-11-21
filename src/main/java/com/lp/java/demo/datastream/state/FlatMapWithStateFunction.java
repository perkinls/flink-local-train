//package com.lp.java.demo.datastream.state;
//
//import com.lp.java.demo.commons.po.SensorPo;
//import com.lp.java.demo.commons.po.config.KafkaConfigPo;
//import com.lp.java.demo.datastream.BaseStreamingEnv;
//import com.lp.java.demo.datastream.IBaseRunApp;
//import com.lp.java.demo.datastream.richfunction.RichMapSplit2Sensor;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.api.java.functions.KeySelector;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
///**
// * @author li.pan
// * @title FlatMapWithState 带状态函数
// * @Date 2021/11/21
// */
//public class FlatMapWithStateFunction extends BaseStreamingEnv<String> implements IBaseRunApp {
//
//    private final static Logger log = LoggerFactory.getLogger(FlatMapWithStateFunction.class);
//
//    @Override
//    public void doMain() throws Exception {
//        FlinkKafkaConsumer<String> kafkaConsumer = getKafkaConsumer(KafkaConfigPo.sensorTopic, new SimpleStringSchema());
//
//        env
//                .addSource(kafkaConsumer)
//                .map(new RichMapSplit2Sensor())
//                .keyBy((KeySelector<SensorPo, String>) SensorPo::getId)
//                .flatMap()
//
//
//    }
//}
