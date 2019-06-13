package com.lp.test.asyncio;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.lp.test.serialization.KafkaEventSchema;
import com.lp.test.utils.ConfigUtils;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import net.sf.json.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import scala.Tuple2;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


/**
 * <p/> 
 * <li>Description: 异步IO写入mysql数据库 JAVA版本(仅供参考)</li>
 * <li>@author: lipan@cechealth.cn</li> 
 * <li>Date: 2019-06-13 23:28</li> 
 * 异步IO三步曲：
 *     1.AsyncFunction的一个实现，用来分派请求
 *     2.获取操作结果并将其传递给ResultFuture的回调
 *     3.将异步I/O操作作为转换应用于DataStream
 *
 */
public class AsyncIoSideTableJoinMysqlJava {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
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

//		设置重启策略
//        env.setRestartStrategy(RestartStrategies.
//                fixedDelayRestart(5,//5次尝试
//                        50000)); //每次尝试间隔50s

        // 选择设置事件事件和处理事件
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        Tuple2<String, Properties> propertiesTuple2 = ConfigUtils.apply("json");

        FlinkKafkaConsumer010<JSONObject> kafkaConsumer010 =  new FlinkKafkaConsumer010<>(propertiesTuple2._1,
                new KafkaEventSchema(),
                propertiesTuple2._2);

        DataStreamSource<JSONObject> source = env
                .addSource(kafkaConsumer010);

        SampleAsyncFunction asyncFunction = new SampleAsyncFunction();

        // add async operator to streaming job
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

        env.execute(AsyncIoSideTableJoinMysqlJava.class.getCanonicalName());
    }

    private static class SampleAsyncFunction extends RichAsyncFunction<JSONObject, JSONObject> {
        private transient SQLClient mySQLClient;
        private Cache<String, String> Cache;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            Cache = Caffeine
                    .newBuilder()
                    .maximumSize(1025)
                    .expireAfterAccess(10, TimeUnit.MINUTES)
                    .build();

            JsonObject mySQLClientConfig = new JsonObject();
            mySQLClientConfig.put("url", "jdbc:mysql://localhost:3306/test")
                    .put("driver_class", "com.mysql.jdbc.Driver")
                    .put("max_pool_size", 20)
                    .put("user", "root")
//                    .put("max_idle_time",1000)
                    .put("password", "123456");

            VertxOptions vo = new VertxOptions();
            vo.setEventLoopPoolSize(10);
            vo.setWorkerPoolSize(20);

            Vertx vertx = Vertx.vertx(vo);
            mySQLClient = JDBCClient.createNonShared(vertx, mySQLClientConfig);

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
                return;
            }
            mySQLClient.getConnection(conn -> {
                if (conn.failed()) {
                    //Treatment failures
                    resultFuture.completeExceptionally(conn.cause());
                    return;
                }

                final SQLConnection connection = conn.result();
            /*
                结合自己的查询逻辑，拼凑出相应的sql，然后返回结果。
             */
                String querySql = "SELECT docs FROM testJoin where fruit = '" + key + "'";
                connection.query(querySql, res2 -> {
                    if (res2.failed()) {
                        resultFuture.complete(null);
                        return;
                    }

                    if (res2.succeeded()) {
                        ResultSet rs = res2.result();
                        List<JsonObject> rows = rs.getRows();
                        if (rows.size() <= 0) {
                            resultFuture.complete(null);
                            return;
                        }
                        for (JsonObject json : rows) {
                            String desc = json.getString("docs");
                            input.put("docs", desc);
                            Cache.put(key, desc);
                            resultFuture.complete(Collections.singleton(input));
                        }
                        // Do something with results
                    } else {
                        resultFuture.complete(null);
                    }
                });

                connection.close(done -> {
                    if (done.failed()) {
                        throw new RuntimeException(done.cause());
                    }
                });

            });
        }

    }
}
