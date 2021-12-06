package com.lp.java.demo.datastream.state;

import com.lp.java.demo.commons.po.config.KafkaConfigPo;
import com.lp.java.demo.datastream.BaseStreamingEnv;
import com.lp.java.demo.base.IBaseRunApp;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author li.pan
 * @title FlatMapWithState 带状态函数(仅在scala)
 * @Date 2021/11/21
 */
public class FlatMapWithStateFunction extends BaseStreamingEnv<String> implements IBaseRunApp {

    private final static Logger log = LoggerFactory.getLogger(FlatMapWithStateFunction.class);

    @Override
    public void doMain() throws Exception {
        FlinkKafkaConsumer<String> kafkaConsumer = getKafkaConsumer(KafkaConfigPo.sensorTopic, new SimpleStringSchema());

       // flatMapWithState 仅在scala中涵盖,https://blog.csdn.net/qq_46548855/article/details/107141625


    }
}
