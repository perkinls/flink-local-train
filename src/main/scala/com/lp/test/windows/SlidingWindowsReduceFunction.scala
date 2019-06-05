package com.lp.test.windows

import com.lp.test.utils.ConfigUtils
import org.apache.flink.api.common.functions.{ReduceFunction, RichMapFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
  * <p/> 
  * <li>Description: 滑动窗口内聚合</li>
  *                  以第一个元素作为基准，窗口大小为10和滑动都为10
  * <li>@author: lipan@cechealth.cn</li> 
  * <li>Date: 2019-05-10 16:36</li> 
  */
object SlidingWindowsReduceFunction {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.getConfig.setAutoWatermarkInterval(1000) //watermark间隔时间

    //设置事件事件
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val kafkaConfig = ConfigUtils.apply("kv")

    val kafkaConsumer = new FlinkKafkaConsumer(kafkaConfig._1,
      new SimpleStringSchema(), //自定义反序列化器
      kafkaConfig._2).setStartFromLatest()

    import org.apache.flink.api.scala._
    val reduce = env
      .addSource(kafkaConsumer)
      .map(new RichMapFunction[String, (String, Long)] {
        override def map(value: String): (String, Long) = {
          val splits = value.split(" ")
          println((splits(0), splits(1).toLong))
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
