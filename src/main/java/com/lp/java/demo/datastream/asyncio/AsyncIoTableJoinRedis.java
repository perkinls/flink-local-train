package com.lp.java.demo.datastream.asyncio;

import com.lp.java.demo.datastream.BaseStreamingEnv;
import com.lp.java.demo.datastream.IBaseRunApp;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.TimeUnit;


/**
 * <p/>
 * <li>Description: 异步IO将数据写入redis缓存中 JAVA版本(仅供参考)</li>
 * <li>@author: panli0226@sina.com</li>
 * <li>Date: 2019-05-15 13:42</li>
 */
public class AsyncIoTableJoinRedis extends BaseStreamingEnv<JSONObject> implements IBaseRunApp {
    private static final Logger log = LoggerFactory.getLogger(AsyncIoTableJoinRedis.class);

    @Override
    public void doMain() throws Exception {

        FlinkKafkaConsumer<JSONObject> kafkaConsumer =
                getKafkaConsumer(KafkaConfigPo.jsonTopic, new JsonDeserializationSchema());

        DataStreamSource<JSONObject> source = env.addSource(kafkaConsumer);

        // AsyncFunction 不是以多线程方式调用的。 只有一个 AsyncFunction 实例
        SampleAsyncFunction asyncFunction = new SampleAsyncFunction();

        // DataStream中增加异步操作
        DataStream<JSONObject> result;
        if (true) {
            result = AsyncDataStream.orderedWait( //连续两个 watermark 之间的记录顺序也被保留了。开销与使用处理时间 相比，没有显著的差别。
                    source,
                    asyncFunction, // 实现异步IO函数
                    1000000L, // 超时参数定义了异步请求发出多久后未得到响应即被认定为失败。 它可以防止一直等待得不到响应的请求。
                    TimeUnit.MILLISECONDS,
                    20);// 容量参数定义了可以同时进行的异步请求数。 即使异步 I/O 通常带来更高的吞吐量，执行异步 I/O 操作的算子仍然可能成为流处理的瓶颈。 限制并发请求的数量可以确保算子不会持续累积待处理的请求进而造成积压，而是在容量耗尽时触发反压。
        } else {
            result = AsyncDataStream.unorderedWait(//Watermark 既不超前于记录也不落后于记录，即 watermark 建立了顺序的边界。 只有连续两个 watermark 之间的记录是无序发出的。
                    source,
                    asyncFunction, // 实现异步IO函数
                    10000,  // 超时参数定义了异步请求发出多久后未得到响应即被认定为失败。 它可以防止一直等待得不到响应的请求。
                    TimeUnit.MILLISECONDS,
                    20);// 容量参数定义了可以同时进行的异步请求数。 即使异步 I/O 通常带来更高的吞吐量，执行异步 I/O 操作的算子仍然可能成为流处理的瓶颈。 限制并发请求的数量可以确保算子不会持续累积待处理的请求进而造成积压，而是在容量耗尽时触发反压。
        }

        result.print();

        env.execute(JobConfigPo.jobNamePrefix + AsyncIoTableJoinRedis.class.getName());

    }

    /**
     * 实现 'AsyncFunction' 用于发送请求和设置回调。
     */
    private static class SampleAsyncFunction extends RichAsyncFunction<JSONObject, JSONObject> {
        private static final long serialVersionUID = -7935595390058358940L;
        private transient RedisClient redisClient;

        @Override
        public void open(Configuration parameters) throws Exception {
            log.info("==============================AsyncFunction open==============================");

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
            log.info("==============================AsyncFunction close==============================");
            if (redisClient != null)
                redisClient.close(null);
        }

        @Override
        public void asyncInvoke(final JSONObject input, final ResultFuture<JSONObject> resultFuture) {
            log.info("==============================AsyncFunction asyncInvoke==============================");
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

        // 当异步 I/O 请求超时的时候，默认会抛出异常并重启作业。 如果你想处理超时，可以重写 AsyncFunction#timeout 方法。
        @Override
        public void timeout(JSONObject input, ResultFuture<JSONObject> resultFuture) throws Exception {

        }
    }
}
