package com.lp.java.demo.datastream.asyncio;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.lp.java.demo.datastream.watermark.KafkaEventSchema;
import com.lp.scala.demo.utils.ConfigUtils;
import net.sf.json.JSONObject;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;
import scala.Tuple2;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * <p/>
 * <li>Description: mapFunction算子同步访问外部存储</li>
 * <li>@author: panli0226@sina.com</li>
 * <li>Date: 2019-06-14 22:22</li>
 * Caffeine可参考 https://www.cnblogs.com/liujinhua306/p/9808500.html
 *
 * 异步访问外部存储，未传递给下游算子。会被足赛
 */
public class FlatMapJoinJava {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置最少一次处理语义和恰一次处理语义
//		env.enableCheckpointing(20000,CheckpointingMode.EXACTLY_ONCE);
//		checkpoint 也可以分开设置
//		env.enableCheckpointing(20000);
//		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
//		设置checkpoint目录
//		env.setStateBackend(new FsStateBackend("/hdfs/checkpoint"));
//        env.getCheckpointConfig() // checkpoint的清楚策略
//                .enableExternalizedCheckpoints(CheckpointConfig.
//                        ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        /**
         * 设置重启策略/5次尝试/每次尝试间隔50s
         */
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 50000));

        // 选择设置事件事件和处理事件
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        Tuple2<String, Properties> propertiesTuple2 = ConfigUtils.apply("json");

        FlinkKafkaConsumer<JSONObject> kafkaConsumer = new FlinkKafkaConsumer<>(
                propertiesTuple2._1,
                new KafkaEventSchema(),
                propertiesTuple2._2);

        SingleOutputStreamOperator<JSONObject> flatMap =
                env
                .addSource(kafkaConsumer)
                .setParallelism(4)
                /**
                 * KeyBy的目的将相同key分配到相同的并行度
                 */
                .keyBy(new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject value) throws Exception {
                        return value.getString("fruit");
                    }
                })

                .flatMap(new JoinWithRedis());

        flatMap.print();
        env.execute(FlatMapJoinJava.class.getCanonicalName());
    }

    public static class JoinWithRedis extends RichFlatMapFunction<JSONObject, JSONObject> {
        private Jedis jedis = null;
        private Cache<String, String> Cache;

        // 打开数据库链接
        @Override
        public void open(Configuration parameters) throws Exception {
            jedis = new Jedis("localhost", 6379);
            // Cache就是本地缓存，缓存超时时间是10min
            Cache = Caffeine
                    .newBuilder()
                    .maximumSize(1025)
                    .expireAfterAccess(10, TimeUnit.MINUTES)
                    .build();
        }

        // 关闭数据库链接
        @Override
        public void close() throws Exception {
            if (jedis != null && jedis.isConnected()) {
                jedis.close();
            }
            if (Cache != null)
                Cache.cleanUp();
        }

        @Override
        public void flatMap(JSONObject value, Collector<JSONObject> out) throws Exception {
            if (jedis != null && (!jedis.isConnected())) {
                String fruit = value.getString("fruit");
                // 本地缓存获取数据
                String cacheData = Cache.getIfPresent(fruit);
                if (null != cacheData) {
                    value.put("docs", cacheData);
                    out.collect(value);
                } else {
                    String s = jedis.get(fruit);
                    if (s != null) {
                        Cache.put(fruit, s);
                        value.put("docs", s);
                    } else {
                        jedis.set(fruit, value.toString());
                        Cache.put(fruit, value.toString());
                    }
                    out.collect(value);
                }
            }

        }
    }
}
