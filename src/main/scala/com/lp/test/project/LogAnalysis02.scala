package com.lp.test.project

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * <p/> 
  * <li>Description:
  * 日志分析系统
  * 功能二：
  *   统计一分钟内每个用户产生的流量统计
  *   域名和用户有对应关系
  *   Flink接受Kafka数据处理+Flink读取域名和用户的配置数据
  *
  * <li>@author: lipan@cechealth.cn</li> 
  * <li>Date: 2019-04-29 20:10</li> 
  */
object LogAnalysis02 {

  val logger = LoggerFactory.getLogger("LogAnalysis02")

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    import org.apache.flink.api.scala._
    val topic = "project_test"

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "master:9092")
    properties.setProperty("group.id", "test-group")

    val consumer = new FlinkKafkaConsumer[String](topic,
                      new SimpleStringSchema(),
                      properties)

    // 接收Kafka数据
    val data = env.addSource(consumer)

    //数据清洗
    val logData = data.map(x => {
      val splits = x.split("\t")
      val level = splits(2)
      val timeStr = splits(3)
      var time = 0l
      try {
        val sourceFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        time = sourceFormat.parse(timeStr).getTime
      } catch {
        case e: Exception => {
          logger.error(s"time parse error: $timeStr", e.getMessage)
        }
      }
      val domain = splits(5)
      val traffic = splits(6).toLong

      (level, time, domain, traffic)
    }).filter(_._2 != 0).filter(_._1 == "E")
      .map(x => {
        (x._2, x._3, x._4)   // 1 level(抛弃)  2 time  3 domain   4 traffic
      })

    //logData.print().setParallelism(1)
    val mysqlData = env.addSource(new MySQLSource)
    //mysqlData.print()
    val connectData = logData.connect(mysqlData)
      .flatMap(new CoFlatMapFunction[(Long,String,Long),mutable.HashMap[String,String],String] {
        var userDomainMap = mutable.HashMap[String,String]()
        // log
        override def flatMap1(value: (Long, String, Long), out: Collector[String]): Unit = {
          val domain = value._2
          val userId = userDomainMap.getOrElse(domain, "")
          println("~~~~~" + userId)
          out.collect(value._1 + "\t" + value._2 + "\t" + value._3 + "\t" + userId)
        }

        // MySQL
        override def flatMap2(value: mutable.HashMap[String, String], out: Collector[String]): Unit = {
          userDomainMap = value
        }
      })

    connectData.print()

    env.execute("LogAnalysis")
  }
}
