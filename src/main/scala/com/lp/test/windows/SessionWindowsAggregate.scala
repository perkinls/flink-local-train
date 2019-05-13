//package com.lp.test.windows
//
//import java.util.Properties
//
//import com.lp.test.trigger.AverageAggregateTrigger
//import org.apache.flink.api.common.functions.RichMapFunction
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.streaming.api.windowing.assigners.{DynamicProcessingTimeSessionWindows, SessionWindowTimeGapExtractor}
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
//
///**
//  * <p/> 
//  * <li>Description: 具有动态间隙的处理时间会话窗口</li>
//  * <li>@author: lipan@cechealth.cn</li> 
//  * <li>Date: 2019-05-10 16:34</li> 
//  */
//object SessionWindowsAggregate {
//
//  def main(args: Array[String]): Unit = {
//
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//
//    env.getConfig.setAutoWatermarkInterval(1000) //watermark间隔时间
//
//    //设置事件事件
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    val props = new Properties()
//    props.setProperty("bootstrap.server", "master:9092")
//    props.setProperty("group.id", "test")
//
//    val kafkaConsumer = new FlinkKafkaConsumer("fk_json_topic",
//      new SimpleStringSchema(), //自定义反序列化器
//      props)
//    kafkaConsumer
//      .setStartFromLatest()
//
//    import org.apache.flink.api.scala._
//    val aggregate = env
//      .addSource(kafkaConsumer)
//      .map(new RichMapFunction[String, (String, Long)] {
//        override def map(value: String): (String, Long) = {
//          val splits = value.split(" ")
//          (splits(0), splits(1).toLong)
//        }
//      })
//      .keyBy(0) //对于tuple类型可以直接使用下标
//      .window(DynamicProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor[String] {
//      override def extract(element: String): Long = {
//        1000 //事件里
//      }
//    }))
//      .trigger(new AverageAggregateTrigger)
//  }
//
//}
