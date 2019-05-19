package com.lp.test.project

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
  * <p/> 
  * <li>Description:
  * 日志分析系统
  * 功能：
  * 1.最近一分钟每个域名产生的流量统计
  *       Flink接受Kafka数据处理
  *
  * 2.统计一分钟内每个用户产生的流量统计
  *   域名和用户有对应关系
  *       Flink接受Kafka数据处理+Flink读取域名和用户的配置数据
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

    val topic = "project_test"
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "master:9092")
    properties.setProperty("group.id", "test-group")
    val consumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties)

    import org.apache.flink.api.scala._
    //1. 接受来自kafka的数据,配置数据源
    val data = env.addSource(consumer)
    val logData = data.map(x => {   //数据清洗
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

    //2. 设置timestamp和watermark,解决时序性问题
    // AssignerWithPeriodicWatermarks[T] 对应logdata的tuple类型
    logData.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(Long, String, Long)] {
      //最大无序容忍的时间 10s
      val maxOutOfOrderness = 10000L
      //当前最大的TimeStamp
      var currentMaxTimeStamp: Long = _
      /**
        * 设置TimeStamp生成WaterMark
        * @return
        */
      override def getCurrentWatermark: Watermark = {
        new Watermark(currentMaxTimeStamp - maxOutOfOrderness)
      }
      /**
        * 抽取时间
        * @param element
        * @param previousElementTimestamp
        * @return
        */
      override def extractTimestamp(element: (Long, String, Long), previousElementTimestamp: Long): Long = {
        //获取数据的event time
        val timestamp = element._1
        currentMaxTimeStamp = Math.max(timestamp, currentMaxTimeStamp)
        timestamp
      }
    })
      //3. 根据window进行业务逻辑的处理   最近一分钟每个域名产生的流量
      .keyBy(1) // 此处按照域名进行keyBy
      .window(TumblingEventTimeWindows.of(Time.seconds(60)))
      .apply(new WindowFunction[(Long, String, Long), (String, String, Long), Tuple, TimeWindow] {
        override def apply(key: Tuple, window: TimeWindow, input: Iterable[(Long, String, Long)], out: Collector[(String, String, Long)]): Unit = {

          val domain = key.getField(0).toString
          var sum = 0l
          val iterator = input.iterator
          var time: String = _
          while (iterator.hasNext) {
            val next = iterator.next()
            sum += next._3 //traffic求和

            //日期去掉秒，随便取一个时间转为（yyy-MM-dd HH:mm)即可
            time = new SimpleDateFormat("yyy-MM-dd HH:mm").format(next._1)
          }

          /**
            * 第一个参数：这一分钟的时间
            * 第二个参数：域名
            * 第三个参数：traffic的和
            */
          out.collect((time, domain, sum))
        }
      })

    //todo 11-14

    //4. 结果输出


  }

}
