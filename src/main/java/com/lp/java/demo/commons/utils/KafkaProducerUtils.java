package com.lp.java.demo.commons.utils;

import com.lp.java.demo.commons.po.SensorPo;
import net.sf.json.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;

/**
 * @author li.pan
 * @version 1.0.0
 * @title Kafka生产者Mock生成数据
 * @createTime 2021年03月08日 18:37:00
 */
public class KafkaProducerUtils {

    private final static Logger log = LoggerFactory.getLogger(KafkaProducerUtils.class);
    private static KafkaProducer<String, String> producer;
    private static Random random = new Random();
    private static ArrayList<String> fruitList = new ArrayList<>(
            Arrays.asList("apple", "pear", "nut", "grape", "banana", "pineapple", "pomelo", "orange")
    );

    /**
     * 关于ProducerRecord的几个点:
     * 1. 若指定Partition ID,则PR被发送至指定Partition
     * 2. 若未指定Partition ID,但指定了Key, PR会按照hasy(key)发送至对应Partition
     * 3. 若既未指定Partition ID也没指定Key，PR会按照round-robin模式发送到每个Partition
     * 4. 若同时指定了Partition ID和Key, PR只会发送到指定的Partition (Key不起作用，代码逻辑决定)
     */

    static {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("acks", "all");
        props.setProperty("retries", "0");
        props.setProperty("batch.size", "16384");
        props.setProperty("linger.ms", "1");
        props.setProperty("buffer.memory", "33554432");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        producer = new KafkaProducer<>(props);
    }
    


    /**
     * Mock传感器数据
     */
    public static void sendMsgSensor() throws InterruptedException {
        while (true) {
            try {
                String msg = new SensorPo(
                        "sensor-" + random.nextInt(3), System.currentTimeMillis(), DoubleUtils.nextDouble(40, 100)
                ).toString();

                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("fk_sensor_topic", msg);
                Future<RecordMetadata> send = producer.send(producerRecord);
                log.info("Mock data:({}) ,Kafka topic: ({}) ,Current offset: ({})", msg, send.get().topic(), send.get().offset());
            } catch (Exception e) {
                log.error("Mock data error, " + e.getMessage());
            } finally {
                Thread.sleep(1000);
            }
        }
    }


    /**
     * Mock Event格式数据
     */
    public static void sendMsgEvent() throws InterruptedException {
        while (true) {
            try {
                String msg = fruitList.get(random.nextInt(8)) + "," + random.nextInt(5) + "," + System.currentTimeMillis();
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("fk_event_topic", msg);
                Future<RecordMetadata> send = producer.send(producerRecord);
                log.info("Mock data:({}) ,Kafka topic: ({}) ,Current offset: ({})", msg, send.get().topic(), send.get().offset());
            } catch (Exception e) {
                log.error("Mock data error, " + e.getMessage());
            } finally {
                Thread.sleep(1000);
            }
        }

    }

    /**
     * Mock key/value格式数据
     */
    public static void sendMsgKv() throws InterruptedException {
        while (true) {
            for (int i = 0; i <= 10; i++) {
                try {

                    // join情况  join和非join开一个即可
//                    String msg = fruitList.get(random.nextInt(8)) + " " + random.nextInt(5) + " " + System.currentTimeMillis();
//                    producer.send(new ProducerRecord<>("fk_kv1_topic", i + "01", msg));
//                    log.info("Mock data fk_kv1_topic :{} ", msg);
//                    producer.send(new ProducerRecord<>("fk_kv2_topic", i + "02", msg));
//                    log.info("Mock data fk_kv2_topic :{} ", msg);

                    // 非join情况
                    String msg = fruitList.get(random.nextInt(8)) + " " + random.nextInt(5);
                    producer.send(new ProducerRecord<>("fk_kv1_topic", String.valueOf(i), msg));
                    log.info("Mock data fk_kv1_topic :{} ", msg);

                    msg = fruitList.get(random.nextInt(8)) + " " + random.nextInt(5);
                    producer.send(new ProducerRecord<>("fk_kv2_topic", String.valueOf(i), msg));
                    log.info("Mock data fk_kv2_topic :{} ", msg);

                } catch (Exception e) {
                    log.error("Mock data error, " + e.getMessage());
                } finally {
                    Thread.sleep(2000);
                }

            }
        }
    }


    /**
     * Mock Json格式数据
     */
    public static void sendMsgJson() throws InterruptedException {
        while (true) {
            try {
                JSONObject resJson = new JSONObject();
                resJson.put("fruit", fruitList.get(random.nextInt(8)));
                resJson.put("number", random.nextInt(4));
                resJson.put("time", System.currentTimeMillis());

                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("fk_json_topic", resJson.toString());
                Future<RecordMetadata> send = producer.send(producerRecord);

                log.info("Mock data:({}) ,Kafka topic: ({}) ,Current offset: ({})", resJson.toString(), send.get().topic(), send.get().offset());
            } catch (Exception e) {
                log.error("Mock data error, " + e.getMessage());
            } finally {
                Thread.sleep(1000);
            }
        }
    }

    /**
     * Mock 字符串格式数据
     */
    public static void sendMsgString() throws InterruptedException {
        while (true) {
            try {
                // 随机生成[0,100)的数字
                String msg = String.valueOf(random.nextInt(100));
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("fk_string_topic", msg);
                Future<RecordMetadata> send = producer.send(producerRecord);
                log.info("Mock data:({}) ,Kafka topic: ({}) ,Current offset: ({})", msg, send.get().topic(), send.get().offset());
            } catch (Exception e) {
                log.error("Mock data error, " + e.getMessage());
            } finally {
                Thread.sleep(1000);
            }
        }
    }


}
