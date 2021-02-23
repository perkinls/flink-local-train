package com.lp.java.demo.datastream.asyncio;

import com.lp.java.demo.commons.BaseStreamingEnv;
import com.lp.java.demo.commons.IBaseRunApp;
import com.lp.java.demo.commons.po.config.JobConfigPo;
import com.lp.java.demo.commons.po.config.KafkaConfigPo;
import com.lp.java.demo.commons.po.config.RedisConfigPo;
import com.lp.java.demo.datastream.source.serialization.JsonDeserializationSchema;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisOptions;
import net.sf.json.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Collections;
import java.util.concurrent.TimeUnit;


/**
 * <p/>
 * <li>Description: 异步IO将数据写入redis缓存中 JAVA版本(仅供参考)</li>
 * <li>@author: panli0226@sina.com</li>
 * <li>Date: 2019-05-15 13:42</li>
 */
public class AsyncIoTableJoinRedis extends BaseStreamingEnv<JSONObject> implements IBaseRunApp {

    @Override
    public void doMain() throws Exception {

        FlinkKafkaConsumer<JSONObject> kafkaConsumer =
                getKafkaConsumer(KafkaConfigPo.jsonTopic, new JsonDeserializationSchema());

        DataStreamSource<JSONObject> source = env.addSource(kafkaConsumer);

        SampleAsyncFunction asyncFunction = new SampleAsyncFunction();

        // DataStream中增加异步操作
        DataStream<JSONObject> result;
        if (true) {
            result = AsyncDataStream.orderedWait(
                    source,
                    asyncFunction,
                    1000000L,
                    TimeUnit.MILLISECONDS,
                    20).setParallelism(1);
        } else {
            result = AsyncDataStream.unorderedWait(
                    source,
                    asyncFunction,
                    10000,
                    TimeUnit.MILLISECONDS,
                    20).setParallelism(1);
        }

        result.print();

        env.execute(JobConfigPo.jobNamePrefix + AsyncIoTableJoinRedis.class.getName());

    }

    private static class SampleAsyncFunction extends RichAsyncFunction<JSONObject, JSONObject> {
        private static final long serialVersionUID = -7935595390058358940L;
        private transient RedisClient redisClient;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            // 构造数据库的连接信息
            RedisOptions config = new RedisOptions();
            config.setHost(RedisConfigPo.host);
            config.setPort(RedisConfigPo.port);

            VertxOptions vo = new VertxOptions();
            vo.setEventLoopPoolSize(10);
            vo.setWorkerPoolSize(20);

            Vertx vertx = Vertx.vertx(vo);
            // 创建Redis客户端
            redisClient = RedisClient.create(vertx, config);
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (redisClient != null)
                redisClient.close(null);

        }

        @Override
        public void asyncInvoke(final JSONObject input, final ResultFuture<JSONObject> resultFuture) {

            String fruit = input.getString("fruit");

            // 直接通过key获取值，可以类比
            redisClient.get(fruit, getRes -> {
                if (getRes.succeeded()) {
                    String result = getRes.result();
                    if (result == null) {
                        resultFuture.complete(Collections.EMPTY_LIST);
                    } else {
                        input.put("docs", result);
                        resultFuture.complete(Collections.singleton(input));
                    }
                } else if (getRes.failed()) {
                    resultFuture.complete(Collections.EMPTY_LIST);
                }
            });
        }

    }
}
