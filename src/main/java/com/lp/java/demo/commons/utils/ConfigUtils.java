package com.lp.java.demo.commons.utils;

import com.lp.java.demo.commons.po.config.*;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author li.pan
 * @version 1.0.0
 * @Description 加载配置文件
 * @createTime 2021年02月08日 11:59:00
 */
public class ConfigUtils {

    private final static Logger log = LoggerFactory.getLogger(ConfigUtils.class);

    private static Config rootConfig = ConfigFactory.load("conf/application");

    private static String configType;

    static {
        configType = rootConfig.getString("init-config-type");
    }

    /**
     * load初始化加载加载
     */
    public static void initLoadConfig() {
        Config eleConfig = rootConfig.getConfig(configType);
        loadKafkaConfig(eleConfig);
        loadJobConfig(eleConfig);
        loadMysqlConfig(eleConfig);
        loadRedisConfig(eleConfig);
        loadFileConfig(eleConfig);
        log.info("加载配置文件初始化完成 ...");
    }

    private static void loadFileConfig(Config pConfig) {
        Config fileConfig = pConfig.getConfig("file");
        FileConfigPo.localFile=fileConfig.getString("local-file-dir");
        FileConfigPo.hdfsFile=fileConfig.getString("hdfs-file");
    }

    /**
     * 加载Kafka相关配置
     *
     * @param pConfig kafka元素节点父节点
     */
    private static void loadKafkaConfig(Config pConfig) {
        Config kafkaConfig = pConfig.getConfig("kafka");
        KafkaConfigPo.bootstrapServers = kafkaConfig.getString("bootstrap.servers");
        KafkaConfigPo.batchSize = kafkaConfig.getLong("batch.size");
        KafkaConfigPo.lingerMs = kafkaConfig.getInt("linger.ms");
        KafkaConfigPo.bufferMemory = kafkaConfig.getLong("buffer.memory");
        KafkaConfigPo.keySerializer = kafkaConfig.getString("key.serializer");
        KafkaConfigPo.valueSerializer = kafkaConfig.getString("value.serializer");
        KafkaConfigPo.groupId = kafkaConfig.getString("group.id");
        KafkaConfigPo.partitionDiscoverMillis = kafkaConfig.getLong("partition.discover.millis");

        Config topicConfig = kafkaConfig.getConfig("topic");
        KafkaConfigPo.stringTopic = topicConfig.getString("string.topic");
        KafkaConfigPo.jsonTopic = topicConfig.getString("json.topic");
        KafkaConfigPo.kvTopic1 = topicConfig.getString("kv1.topic");
        KafkaConfigPo.kvTopic2 = topicConfig.getString("kv2.topic");
        KafkaConfigPo.eventTopic = topicConfig.getString("event.topic");
        KafkaConfigPo.sensorTopic = topicConfig.getString("sensor.topic");
    }

    /**
     * 加载当前任务相关配置
     *
     * @param pConfig job元素节点父节点
     */
    private static void loadJobConfig(Config pConfig) {
        Config jobConfig = pConfig.getConfig("job");
        JobConfigPo.jobNamePrefix = jobConfig.getString("name.prefix");
        JobConfigPo.defaultParallelism = jobConfig.getInt("default.parallelism");
        JobConfigPo.sinkParallelism = jobConfig.getInt("sink.parallelism");

        Config checkpointConfig = jobConfig.getConfig("checkpoint");
        JobConfigPo.enableCheckpoint = checkpointConfig.getBoolean("enable");
        JobConfigPo.checkpointInterval = checkpointConfig.getLong("interval");
        JobConfigPo.checkpointType = checkpointConfig.getString("type");
        JobConfigPo.checkPointPath = checkpointConfig.getString("path");
        JobConfigPo.checkpointTimeOut = checkpointConfig.getLong("timeout");
        JobConfigPo.checkpointMinPauseBetween = checkpointConfig.getLong("min.pause.between");
        JobConfigPo.checkpointCurrentCheckpoints = checkpointConfig.getInt("current.checkpoints");
        JobConfigPo.checkpointRestartAttempts = checkpointConfig.getInt("restart.attempts.times");
        JobConfigPo.checkpointRestartAttemptsInterval = checkpointConfig.getLong("restart.attempts.interval");

    }

    /**
     * 加载Mysql相关配置
     *
     * @param pConfig job元素节点父节点
     */
    private static void loadMysqlConfig(Config pConfig) {
        Config mysqlConfig = pConfig.getConfig("mysql");
        MysqlConfigPo.url = mysqlConfig.getString("url");
        MysqlConfigPo.driver = mysqlConfig.getString("driver");
        MysqlConfigPo.maxPoolSize = mysqlConfig.getInt("max-pool-size");
        MysqlConfigPo.maxIdleTime = mysqlConfig.getInt("max-idle-time");
        MysqlConfigPo.user = mysqlConfig.getString("user");
        MysqlConfigPo.password = mysqlConfig.getString("password");
    }

    /**
     * 加载Mysql相关配置
     *
     * @param pConfig job元素节点父节点
     */
    private static void loadRedisConfig(Config pConfig) {
        Config redisConfig = pConfig.getConfig("redis");
        RedisConfigPo.host = redisConfig.getString("host");
        RedisConfigPo.port = redisConfig.getInt("port");

    }
}