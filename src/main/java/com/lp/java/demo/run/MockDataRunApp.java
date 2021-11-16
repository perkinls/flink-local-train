package com.lp.java.demo.run;

import com.lp.java.demo.commons.utils.KafkaProducerUtils;

/**
 * @author li.pan
 * @version 1.0.0
 * @title kafka生产者Mock生产数据
 * @createTime 2021年03月09日 13:47:00
 */
public class MockDataRunApp {

    public static void main(String[] args) throws InterruptedException {

//        KafkaProducerUtils.sendMsgString();
        KafkaProducerUtils.sendMsgJson();
//        KafkaProducerUtils.sendMsgKv();
//        KafkaProducerUtils.sendMsgEvent();
//        KafkaProducerUtils.sendMsgSensor();
    }
}
