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
  *
  * <li>@author: lipan@cechealth.cn</li> 
  * <li>Date: 2019-04-29 20:10</li> 
  */
object LogAnalysis02 {

  // 在生产上记录日志建议采用这种方式
  val logger = LoggerFactory.getLogger("LogAnalysis02")

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    import org.apache.flink.api.scala._
    val topic = "pktest"

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.199.233:9092")
    properties.setProperty("group.id", "test-pk-group")

    val consumer = new FlinkKafkaConsumer[String](topic,
                      new SimpleStringSchema(),
                      properties)

    // 接收Kafka数据
    val data = env.addSource(consumer)

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

    /**
      * 在生产上进行业务处理的时候，一定要考虑处理的健壮性以及你数据的准确性
      * 脏数据或者是不符合业务规则的数据是需要全部过滤掉之后
      * 再进行相应业务逻辑的处理
      *
      * 对于我们的业务来说，我们只需要统计level=E的即可
      * 对于level非E的，不做为我们业务指标的统计范畴
      *
      * 数据清洗：就是按照我们的业务规则把原始输入的数据进行一定业务规则的处理
      * 使得满足我们的业务需求为准
      */

    //logData.print().setParallelism(1)


    val mysqlData = env.addSource(new PKMySQLSource)
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
