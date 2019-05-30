package com.lp.test.project

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
  * <p/> 
  * <li>Description:
  * 日志分析系统
  * 功能：
  *   最近一分钟每个域名产生的流量统计
  *
  * <li>@author: lipan@cechealth.cn</li> 
  * <li>Date: 2019-04-28 20:45</li> 
  */
object LogAnalysis {

  val logger = LoggerFactory.getLogger("LogAnalysis")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置事件时间作为flink处理的基准时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    //设置最少一次和恰一次处理语义
//    env.enableCheckpointing(20000, CheckpointingMode.EXACTLY_ONCE)
//
//    env.getCheckpointConfig.enableExternalizedCheckpoints(
//      CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
//    //设置重启策略
//    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, //5次尝试
//      50000)) //每次尝试间隔50s

    val topic = "project_test"
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "master:9092")
    properties.setProperty("group.id", "test-group")
    val consumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties)
      .setStartFromLatest()

    import org.apache.flink.api.scala._
    val value: DataStream[String] = env.addSource(consumer)
    //1. 接受来自kafka的数据,配置数据源
    val data = value
    //2.数据清洗
    val logData = data.map(x => {
      val splits = x.split("\t")
      val level = splits(2)
      val timeStr = splits(3)
      var time = 0l
      //时间处理
      try {
        val sourceFormat = new SimpleDateFormat("yyy-MM-dd HH:mm:ss")
        time = sourceFormat.parse(timeStr).getTime
      } catch {
        case e: Exception => {
          logger.error(s"time parse error $timeStr", e.getMessage)
        }
      }
      val domain = splits(5)
      val traffic = splits(6).toLong
      (level, time, domain, traffic)
    }).filter(_._2 != 0).filter(_._1 == "E") //过滤掉时间为0和level非"E"的数据
      .map(x => {
      (x._2, x._3, x._4) //数据清洗按照业务规则取相关数据 1level(不需要可以抛弃) 2time 3 domain 4traffic
    })

    //3. 设置timestamp和watermark,解决时序性问题
    // AssignerWithPeriodicWatermarks[T] 对应logdata的tuple类型
    val resultData = logData.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(Long, String, Long)] {
      //最大无序容忍的时间 10s
      val maxOutOfOrderness = 10000L
      //当前最大的TimeStamp
      var currentMaxTimeStamp: Long = _
      /**
        * 设置TimeStamp生成WaterMark
        */
      override def getCurrentWatermark: Watermark = {
        new Watermark(currentMaxTimeStamp - maxOutOfOrderness)
      }
      /**
        * 抽取时间
        */
      override def extractTimestamp(element: (Long, String, Long), previousElementTimestamp: Long): Long = {
        //获取数据的event time
        val timestamp = element._1
        currentMaxTimeStamp = Math.max(timestamp, currentMaxTimeStamp)
        timestamp
      }
    })
      //4. 根据window进行业务逻辑的处理   最近一分钟每个域名产生的流量
      .keyBy(1) // 此处按照域名进行keyBy
      .window(TumblingEventTimeWindows.of(Time.seconds(60)))
      .apply(new WindowFunction[(Long, String, Long), (String, String, Long), Tuple, TimeWindow] {
        override def apply(key: Tuple, window: TimeWindow, input: Iterable[(Long, String, Long)], out: Collector[(String, String, Long)]): Unit = {

          val domain = key.getField(0).toString
          var sum = 0l
          val times = ArrayBuffer[Long]()

          val iterator = input.iterator
          while (iterator.hasNext) {
            val next = iterator.next()
            sum += next._3         // traffic流量字段求和
            times.append(next._1)
          }

          /** 第一个参数：这一分钟的时间 2019-09-09 20:20 第二个参数：域名 第三个参数：traffic的和 */
          val time = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(new Date(times.max))
          out.collect((time, domain, sum))
        }
      })

    //5.数据Sink写入ES
    val httpHosts = new java.util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost("master", 9200, "http"))
    val esSinkBuilder = new ElasticsearchSink.Builder[(String, String, Long)](
      httpHosts,
      new ElasticsearchSinkFunction[(String, String, Long)] {

        def createIndexRequest(element: (String, String, Long)): IndexRequest = {
          val json = new java.util.HashMap[String, Any]
          json.put("time", element._1)
          json.put("domain", element._2)
          json.put("traffics", element._3)

          //保存到ES中的id
          val id = element._1 + "-" + element._2
          return Requests.indexRequest()
            .index("cdn_project")
            .`type`("traffic")
            .id(id)
            .source(json)
        }

        override def process(t: (String, String, Long), runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          requestIndexer.add(createIndexRequest(t))
        }
      }
    )
    //设置要为每个批量请求缓冲的最大操作数
    esSinkBuilder.setBulkFlushMaxActions(1)
    resultData.addSink(esSinkBuilder.build) //.setParallelism(5)
    env.execute("LogAnalysis")
  }
}