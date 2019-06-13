//package com.lp.test.asyncio
//
//import java.util.Collections
//import java.util.concurrent.TimeUnit
//
//import com.github.benmanes.caffeine.cache.Cache
//import com.lp.test.serialization.KafkaEventSchema
//import com.lp.test.utils.ConfigUtils
//import io.vertx.core.{AsyncResult, Handler, Vertx, VertxOptions}
//import io.vertx.redis.{RedisClient, RedisOptions}
//import net.sf.json.JSONObject
//import org.apache.flink.api.common.restartstrategy.RestartStrategies
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.datastream.{AsyncDataStream, DataStream}
//import org.apache.flink.streaming.api.environment.{CheckpointConfig, StreamExecutionEnvironment}
//import org.apache.flink.streaming.api.functions.async.{ResultFuture, RichAsyncFunction}
//import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
//
///**
//  * <p/> 
//  * <li>Description: 异步IO将数据写入redis缓存中</li>
//  * <li>@author: lipan@cechealth.cn</li> 
//  * <li>Date: 2019-05-15 13:42</li> 
//  * 注意：
//  * vertx目前只支持scala 2.12的版本,该demo不能编译通过，请参考java版本
//  *             <dependency>
//  *             <groupId>io.vertx</groupId>
//  *             <artifactId>vertx-lang-scala_2.12</artifactId>
//  *             <version>3.5.4</version>
//  *             </dependency>
//  */
//object AsyncIOSideTableJoinRedis {
//  def main(args: Array[String]): Unit = {
//
//    //构建运行时环境
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//
//    //设置最少一次和恰一次处理语义
//    env.enableCheckpointing(20000, CheckpointingMode.EXACTLY_ONCE)
//
//    //设置checkpoint目录
//    env.getCheckpointConfig.enableExternalizedCheckpoints(
//      CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
//
//    //设置重启策略
//    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, //5次尝试
//      50000)) //每次尝试间隔50s
//    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
//    val kafkaConfig = ConfigUtils.apply("json")
//
//    val kafkaConsumer = new FlinkKafkaConsumer(kafkaConfig._1,
//      new KafkaEventSchema(), //自定义反序列化器
//      kafkaConfig._2)
//      .setStartFromLatest()
//
//    val source: DataStream[JSONObject] = env.addSource(kafkaConsumer)
//    val asyncFunction = new SampleAsyncFunction
//    val result = if (true) {
//      AsyncDataStream.orderedWait(source,
//        asyncFunction,
//        1000000L,
//        TimeUnit.MILLISECONDS,
//        20)
//        .setParallelism(1)
//    } else {
//      AsyncDataStream.unorderedWait(source,
//        asyncFunction,
//        1000000L,
//        TimeUnit.MILLISECONDS,
//        20)
//        .setParallelism(1)
//    }
//
//    result.print()
//
//    env.execute("AsyncIoSideTableJoinRedisJava")
//  }
//
//  /**
//    * AsyncFunction，该函数实现了异步请求分发的功能
//    */
//  class SampleAsyncFunction extends RichAsyncFunction[JSONObject, JSONObject] {
//
//    @transient var redisClient: RedisClient = _
//    var cache: Cache[String, String] = _
//
//    /**
//      * 函数的初始化方法。在实际工作方法之前调用它
//      *
//      * @param parameters
//      */
//    override def open(parameters: Configuration): Unit = {
//      super.open(parameters)
//
//      //初始化redis相关配置
//      val redisConfig = new RedisOptions()
//        .setAddress("127.0.0.1")
//        .setPort(6379)
//      val vo = new VertxOptions()
//        vo.setEventLoopPoolSize(10)
//        vo.setWorkerPoolSize(20)
//
//      val vt = Vertx.vertx(vo)
//      redisClient = RedisClient.create(vt, redisConfig)
//    }
//
//    /**
//      * 用户代码的拆除方法。在最后一次调用。对于作为迭代一部分的函数，将在每次迭代步骤后调用此方法。
//      */
//    override def close(): Unit = {
//      super.close()
//      if (redisClient != null) {
//        redisClient.close(null)
//      }
//    }
//
//    /**
//      * 触发每个流输入的异步操作
//      *
//      * @param input
//      * @param resultFuture
//      */
//    override def asyncInvoke(input: JSONObject, resultFuture: ResultFuture[JSONObject]): Unit = {
//
//      val fruit = input.getString("fruit")
//
//      redisClient.get(fruit, new Handler[AsyncResult[String]] {
//        override def handle(event: AsyncResult[String]): Unit = {
//          if (event.succeeded()) {
//            val result = event.result()
//            if (result == null) {
//              resultFuture.complete(null)
//            } else {
//              input.put("docs", result)
//              resultFuture.complete(Collections.singleton(input))
//            }
//          } else {
//            resultFuture.complete(null)
//          }
//        }
//      })
//
//    }
//  }
//
//}
