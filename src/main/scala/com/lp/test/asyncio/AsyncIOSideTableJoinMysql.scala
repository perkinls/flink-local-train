//package com.lp.test.asyncio
//
//import java.util
//import java.util.Collections
//import java.util.concurrent.TimeUnit
//
//import com.github.benmanes.caffeine.cache.{Cache, Caffeine}
//import com.lp.test.utils.ConfigUtils
//import io.vertx.core.json.JsonObject
//import io.vertx.core.{AsyncResult, Handler, Vertx, VertxOptions}
//import io.vertx.ext.jdbc.JDBCClient
//import io.vertx.ext.sql.{ResultSet, SQLConnection}
//import net.sf.json.JSONObject
//import org.apache.flink.api.common.restartstrategy.RestartStrategies
//import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}
//import org.apache.flink.api.common.typeinfo.TypeInformation
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.datastream.{AsyncDataStream, DataStream}
//import org.apache.flink.streaming.api.environment.{CheckpointConfig, StreamExecutionEnvironment}
//import org.apache.flink.streaming.api.functions.async.{ResultFuture, RichAsyncFunction}
//import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
//
///**
//  * <p/> 
//  * <li>Description: 异步IO写入mysql数据库</li>
//  * <li>@author: lipan@cechealth.cn</li> 
//  * <li>Date: 2019-05-15 13:42</li> 
//  * 异步IO三步曲：
//  *     1.AsyncFunction的一个实现，用来分派请求
//  *     2.获取操作结果并将其传递给ResultFuture的回调
//  *     3.将异步I/O操作作为转换应用于DataStream
//  * 注意：
//  * vertx目前只支持scala 2.12的版本,该demo不能编译通过，请参考java版本
//  * <dependency>
//  * <groupId>io.vertx</groupId>
//  * <artifactId>vertx-lang-scala_2.12</artifactId>
//  * <version>3.5.4</version>
//  * </dependency>
//  *
//  */
//object AsyncIOSideTableJoinMysql {
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
//      new JSONEventSchema, //自定义反序列化器
//      kafkaConfig._2)
//
//
//    //注意DataStream的包 org.apache.flink.streaming.api.datastream
//    val source: DataStream[JSONObject] = env.addSource(kafkaConsumer)
//
//    //进行异步IO操作
//    val result = if (true) {
//      AsyncDataStream.orderedWait(source,
//        new SampleAsyncFunction,
//        1000000L, //异步请求超时时间
//        TimeUnit.MILLISECONDS,
//        20) //异步请求容量
//        .setParallelism(1)
//    } else {
//      AsyncDataStream.unorderedWait(source,
//        new SampleAsyncFunction,
//        1000000L, //异步请求超时时间
//        TimeUnit.MILLISECONDS,
//        20) //异步请求容量
//        .setParallelism(1)
//    }
//
//    result.print()
//
//    env.execute("AsyncIoSideTableJoinMysqlJava")
//
//  }
//
//  /**
//    * AsyncFunction，该函数实现了异步请求分发的功能
//    */
//  class SampleAsyncFunction extends RichAsyncFunction[JSONObject, JSONObject] {
//
//    //mySQLClient就是在可序列化对象里某些成员在序列化时不被序列化
//    @transient var mySQLClient: JDBCClient = _
//    var cache: Cache[String, String] = _
//
//    /**
//      * 函数的初始化方法。在实际工作方法之前调用它
//      *
//      * @param parameters
//      */
//    override def open(parameters: Configuration): Unit = {
//      super.open(parameters)
//      cache = Caffeine
//        .newBuilder()
//        .maximumSize(1025)
//        .expireAfterAccess(10, TimeUnit.MINUTES)
//        .build[String, String]
//
//      val mysqlConfig = new JsonObject()
//        .put("url", "jdbc:mysql://localhost:3306/test")
//        .put("driver_class", "com.mysql.jdbc.Driver")
//        .put("max_pool_size", 20)
//        .put("user", "root")
//        .put("password", "1234")
//      //                    .put("max_idle_time",1000)
//      val vo = new VertxOptions()
//      vo.setEventLoopPoolSize(10)
//      vo.setWorkerPoolSize(20)
//
//      //      val vt = Vertx.vertx(vo)
//      //创建一个维护自己的数据源的JDBC客户端。
//      val vertx: Vertx = GetVertx.getTx()
//      mySQLClient = JDBCClient.createNonShared(vertx, mysqlConfig)
//
//    }
//
//    /**
//      * 用户代码的拆除方法。在最后一次调用。对于作为迭代一部分的函数，将在每次迭代步骤后调用此方法。
//      */
//    override def close(): Unit = {
//      super.close()
//      if (mySQLClient != null) {
//        mySQLClient.close()
//      }
//      if (cache != null) {
//        cache.cleanUp()
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
//      val key = input.getString("fruit")
//      //在缓存中判断key是否存在
//      val cacheIfExist = cache.getIfPresent(key)
//      if (cacheIfExist != null) {
//        input.put("docs", cacheIfExist)
//        resultFuture.complete(Collections.singleton(input))
//        return
//      }
//      mySQLClient.getConnection(new Handler[AsyncResult[SQLConnection]] {
//        override def handle(event: AsyncResult[SQLConnection]): Unit = {
//          if (event.failed()) {
//            resultFuture.complete(null)
//            return
//          }
//          val conn = event.result()
//          //结合业务拼sql
//          val querySql = "SELECT docs FROM testJoin where fruit = '" + key + "'"
//
//          conn.query(querySql, new Handler[AsyncResult[ResultSet]] {
//            override def handle(event: AsyncResult[ResultSet]): Unit = {
//              if (event.failed()) {
//                resultFuture.complete(null)
//                return
//              }
//
//              if (event.succeeded()) { //成功情况下
//                val rs = event.result()
//
//                val rows: util.List[JsonObject] = rs.getRows
//                if (rows.size() <= 0) {
//                  resultFuture.complete(null)
//                  return
//                }
//
//                import scala.collection.JavaConverters._
//                rows.asScala.foreach(e => {
//                  val desc = e.getString("docs")
//                  input.put("docs", desc)
//                  cache.put(key, desc)
//                  resultFuture.complete(Collections.singleton(input))
//                })
//
//              } else { //异常的情况，不进行处理会阻塞
//                resultFuture.complete(null)
//              }
//
//              conn.close(new Handler[AsyncResult[Void]] {
//                override def handle(event: AsyncResult[Void]): Unit = {
//                  if (event.failed()) throw new RuntimeException(event.cause)
//                }
//              })
//            }
//          })
//        }
//      })
//
//    }
//  }
//
//  class JSONEventSchema extends DeserializationSchema[JSONObject] with SerializationSchema[JSONObject] {
//
//    override def deserialize(message: Array[Byte]): JSONObject = {
//      JSONObject.fromObject(new String(message))
//    }
//
//    override def isEndOfStream(nextElement: JSONObject): Boolean = false
//
//    override def serialize(element: JSONObject): Array[Byte] = {
//      element.toString().getBytes()
//    }
//
//    override def getProducedType: TypeInformation[JSONObject] = {
//
//      TypeInformation.of(classOf[JSONObject])
//    }
//  }
//
//}
