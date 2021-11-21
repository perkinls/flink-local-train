package com.lp.java.demo.datastream.asyncio;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.lp.java.demo.datastream.BaseStreamingEnv;
import com.lp.java.demo.datastream.IBaseRunApp;
import com.lp.java.demo.commons.po.config.JobConfigPo;
import com.lp.java.demo.commons.po.config.KafkaConfigPo;
import com.lp.java.demo.commons.po.config.MysqlConfigPo;
import com.lp.java.demo.datastream.source.serialization.JsonDeserializationSchema;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
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
import java.util.List;
import java.util.concurrent.TimeUnit;


/**
 * <p/>
 * <li>Description: 异步IO写入mysql数据库 JAVA版本(仅供参考)</li>
 * <li>@author: panli0226@sina.com</li>
 * <li>Date: 2019-06-13 23:28</li>
 * 异步IO三步曲：
 * 1.AsyncFunction的一个实现，用来分派请求
 * 2.获取操作结果并将其传递给ResultFuture的回调
 * 3.将异步I/O操作作为转换应用于DataStream
 * <p>
 * 可用于高效join维表
 */
public class AsyncIoTableJoinMysql extends BaseStreamingEnv<JSONObject> implements IBaseRunApp {

    private final static Logger log = LoggerFactory.getLogger(BaseStreamingEnv.class);


    @Override
    public void doMain() throws Exception {

        FlinkKafkaConsumer<JSONObject> kafkaConsumer =
                getKafkaConsumer(KafkaConfigPo.jsonTopic, new JsonDeserializationSchema());

        DataStreamSource<JSONObject> source = env.addSource(kafkaConsumer);

        // AsyncFunction 不是以多线程方式调用的。 只有一个 AsyncFunction 实例
        SampleAsyncFunction asyncFunction = new SampleAsyncFunction();

        // DataStream 中增加异步操作
        DataStream<JSONObject> result;
        if (true) {
            result = AsyncDataStream.orderedWait(
                    source,
                    asyncFunction,
                    10000, // 超时参数定义了异步请求发出多久后未得到响应即被认定为失败,防止一直等待得不到响应的请求。
                    TimeUnit.MILLISECONDS,
                    20 // 容量参数定义了可以同时进行的异步请求数。
            ).setParallelism(1);
        } else {
            result = AsyncDataStream.unorderedWait(
                    source,
                    asyncFunction,
                    10000,
                    TimeUnit.MILLISECONDS,
                    20
            ).setParallelism(1);
        }

        result.print();
        env.execute(JobConfigPo.jobNamePrefix + AsyncIoTableJoinMysql.class.getName());

    }

    @Override
    public Integer setDefaultParallelism() {
        return 1;
    }

    /**
     * 实现AsyncFunction，该函数实现了请求分发的功能。
     */
    private static class SampleAsyncFunction extends RichAsyncFunction<JSONObject, JSONObject> {
        private static final long serialVersionUID = 7805186394896067480L;

        private transient SQLClient mySQLClient;
        private Cache<String, String> Cache;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 初始化本地缓存
            Cache = Caffeine
                    .newBuilder()
                    .maximumSize(1025)
                    .expireAfterAccess(10, TimeUnit.MINUTES)
                    .build();

            // 构造数据库的连接信息
            JsonObject mySQLClientConfig = new JsonObject();
            mySQLClientConfig.put("url", MysqlConfigPo.url)
                    .put("driver_class", MysqlConfigPo.driver)
                    .put("max_pool_size", MysqlConfigPo.maxPoolSize)
                    .put("user", MysqlConfigPo.user)
                    .put("max_idle_time", MysqlConfigPo.maxIdleTime)
                    .put("password", MysqlConfigPo.password);

            VertxOptions vo = new VertxOptions();
            vo.setEventLoopPoolSize(10);
            vo.setWorkerPoolSize(20);

            Vertx vertx = Vertx.vertx(vo);
            // 使用异步框架创建Mysql客户端
            mySQLClient = JDBCClient.createNonShared(vertx, mySQLClientConfig);
            log.info("Async IO open init finish");

        }

        @Override
        public void close() throws Exception {
            super.close();
            if (mySQLClient != null)
                mySQLClient.close();
            if (Cache != null)
                Cache.cleanUp();
        }

        @Override
        public void asyncInvoke(final JSONObject input, final ResultFuture<JSONObject> resultFuture) {

            String key = input.getString("fruit");
            String cacheIfPresent = Cache.getIfPresent(key);
            if (cacheIfPresent != null) {
                input.put("docs", cacheIfPresent);
                resultFuture.complete(Collections.singleton(input));
                // return;
            }
            mySQLClient.getConnection(conn -> {
                if (conn.failed()) {
                    //获取连接失败
                    resultFuture.completeExceptionally(conn.cause());
                    // return;
                }

                final SQLConnection connection = conn.result();

                //结合自己的查询逻辑，拼凑出相应的sql，然后返回结果。Mysql中需要存储些数据
                String querySql = "SELECT docs FROM flink_test_join where fruit = '" + key + "'";
                connection.query(querySql, res2 -> {
                    if (res2.failed()) {
                        resultFuture.completeExceptionally(res2.cause());
                    }
                    if (res2.succeeded()) {
                        ResultSet rs = res2.result();
                        List<JsonObject> rows = rs.getRows();
                        if (rows.size() <= 0) {
                            resultFuture.complete(Collections.EMPTY_LIST);
                        }
                        for (JsonObject json : rows) {
                            String desc = json.getString("docs");
                            input.put("docs", desc);
                            Cache.put(key, desc);
                            resultFuture.complete(Collections.singleton(input));
                        }
                    } else {
                        resultFuture.completeExceptionally(res2.cause());
                    }
                });

                connection.close(done -> {
                    if (done.failed()) {
                        resultFuture.completeExceptionally(done.cause());
                    }
                });

            });
        }

    }
}
