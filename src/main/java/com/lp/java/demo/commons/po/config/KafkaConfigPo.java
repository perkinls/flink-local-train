package com.lp.java.demo.commons.po.config;

import java.io.Serializable;

/**
 * @author li.pan
 * @version 1.0.0
 * @Description 配置类
 * @createTime 2021年02月08日 12:24:00
 */
public class KafkaConfigPo implements Serializable {

    private static final long serialVersionUID = 586916524153149480L;

    /**
     * 集群地址
     */
    public static String bootstrapServers;
    /**
     * Batch大小
     */
    public static Long batchSize;
    /**
     * Batch过多久发送出去
     */
    public static Integer lingerMs;
    /**
     * 缓存大小
     */
    public static Long bufferMemory;
    /**
     * key默认序列化方式
     */
    public static String keySerializer;
    /**
     * value默认序列化方式
     */
    public static String valueSerializer;
    /**
     * 消费者组
     */
    public static String groupId;

    /**
     * 间隔多久（interval）获取一次 kafka 的元数据
     */
    public static Long partitionDiscoverMillis;

    /**
     * 单一字符串Topic
     */
    public static String stringTopic;

    /**
     * Json Topic
     */
    public static String jsonTopic;

    /**
     * kv Topic
     */
    public static String kvTopic1;

    /**
     * kv Topic
     */
    public static String kvTopic2;
}
