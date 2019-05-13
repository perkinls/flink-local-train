package com.lp.test.windows

import java.util.Properties

import org.apache.flink.api.common.functions.{ReduceFunction, RichMapFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
  * <p/> 
  * <li>Description: TODO</li>
  * <li>@author: lipan@cechealth.cn</li> 
  * <li>Date: 2019-05-10 16:36</li> 
  */
object SlidingWindowsReduceFunction {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.getConfig.setAutoWatermarkInterval(1000) //watermark间隔时间

    //设置事件事件
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val props = new Properties()
    props.setProperty("bootstrap.server", "master:9092")
    props.setProperty("group.id", "test")

    val kafkaConsumer = new FlinkKafkaConsumer("fk_json_topic",
      new SimpleStringSchema(), //自定义反序列化器
      props)
    kafkaConsumer
      .setStartFromLatest()

    import org.apache.flink.api.scala._
    val reduce = env
      .addSource(kafkaConsumer)
      .map(new RichMapFunction[String, (String, Long)] {
        override def map(value: String): (String, Long) = {
          val splits = value.split(" ")
          (splits(0), splits(1).toLong)
        }
      })
      .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(10)))
      .reduce(new ReduceFunction[(String, Long)] {
        override def reduce(value1: (String, Long), value2: (String, Long)): (String, Long) = {
          (value1._1, value1._2 + value2._2)
        }
      })

    reduce.print()

    env.execute("SlidingWindowsReduceFunction")

  }

}
