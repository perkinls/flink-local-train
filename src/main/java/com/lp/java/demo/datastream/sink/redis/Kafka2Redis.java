package com.lp.java.demo.datastream.sink.redis;

import com.lp.java.demo.datastream.trigger.CustomProcessingTimeTrigger;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.Properties;

/*
    文档：
    https://bahir.apache.org/docs/flink/current/flink-streaming-redis/
 */
public class Kafka2Redis {
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "mt-mdh.local:9093");
        properties.setProperty("group.id", "test");

        FlinkKafkaConsumer010<String> kafkaConsumer010 = new FlinkKafkaConsumer010<>("test",
                new SimpleStringSchema(),
                properties);

        AllWindowedStream<Integer, TimeWindow> stream = env
                .addSource(kafkaConsumer010)
                .map(new Kafka2Redis.String2Integer())
                .timeWindowAll(org.apache.flink.streaming.api.windowing.time.Time.seconds(20))
                .trigger(CustomProcessingTimeTrigger.create());

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();
        //实例化RedisSink，并通过flink的addSink的方式将flink计算的结果插入到redis
        stream.sum(0).addSink(new RedisSink<Integer>(conf, new RedisExampleMapper()));


        env.execute(Kafka2Redis.class.getCanonicalName());
    }
    private static class String2Integer extends RichMapFunction<String, Integer> {
        private static final long serialVersionUID = 1180234853172462378L;
        @Override
        public Integer map(String event) throws Exception {

            return Integer.valueOf(event);
        }
        @Override
        public void open(Configuration parameters) throws Exception {
        }
    }

    public static final class RedisExampleMapper implements RedisMapper<Integer> {
        //设置数据使用的数据结构 Set
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.SET);
        }

        // 指定key
        public String getKeyFromData(Integer data) {
            return "kafka2redis";
        }

        //指定value
        public String getValueFromData(Integer data) {
            return data.toString();
        }
    }

}

