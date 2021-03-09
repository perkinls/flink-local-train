package com.lp.java.demo.datastream.asyncio;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.lp.java.demo.datastream.BaseStreamingEnv;
import com.lp.java.demo.datastream.IBaseRunApp;
import com.lp.java.demo.commons.po.config.JobConfigPo;
import com.lp.java.demo.commons.po.config.KafkaConfigPo;
import com.lp.java.demo.commons.po.config.RedisConfigPo;
import com.lp.java.demo.datastream.source.serialization.JsonDeserializationSchema;
import net.sf.json.JSONObject;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.util.concurrent.TimeUnit;

/**
 * <p/>
 * <li>Description: mapFunction算子同步访问外部存储</li>
 * <li>@author: panli0226@sina.com</li>
 * <li>Date: 2019-06-14 22:22</li>
 * Caffeine可参考 https://www.cnblogs.com/liujinhua306/p/9808500.html
 * <p>
 * 异步访问外部存储，未传递给下游算子。会被足赛
 */
public class AsyncIoFlatMapJoin extends BaseStreamingEnv<JSONObject> implements IBaseRunApp {

    @Override
    public void doMain() throws Exception {

        FlinkKafkaConsumer<JSONObject> kafkaConsumer =
                getKafkaConsumer(KafkaConfigPo.jsonTopic, new JsonDeserializationSchema());


        SingleOutputStreamOperator<JSONObject> flatMap =
                env
                        .addSource(kafkaConsumer)
                        /**
                         * KeyBy的目的将相同key分配到相同的并行度
                         */
                        .keyBy((KeySelector<JSONObject, String>) value -> value.getString("fruit"))
                        .flatMap(new JoinWithRedis());

        flatMap.print();
        env.execute(JobConfigPo.jobNamePrefix+AsyncIoFlatMapJoin.class.getName());

    }

    @Override
    public Integer setDefaultParallelism() {
        return 1;
    }

    public static class JoinWithRedis extends RichFlatMapFunction<JSONObject, JSONObject> {
        private static final long serialVersionUID = -5509536907313253423L;
        private Jedis jedis = null;
        private Cache<String, String> Cache;

        // 打开数据库链接
        @Override
        public void open(Configuration parameters) throws Exception {
            jedis = new Jedis(RedisConfigPo.host, RedisConfigPo.port);
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
