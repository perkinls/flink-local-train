//package com.lp.test.windows
//
//import java.util.Properties
//
//import com.lp.test.trigger.CustomProcessTimeTrigger
//import org.apache.flink.api.common.functions.{FoldFunction, RichMapFunction}
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
//
///**
//  * <p/> 
//  * <li>Description: TODO</li>
//  * <li>@author: lipan@cechealth.cn</li> 
//  * <li>Date: 2019-05-10 16:34</li> 
//  */
//object TumblingWindowsFold {
//  def main(args: Array[String]): Unit = {
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
//    val fold = env
//      .addSource(kafkaConsumer)
//      .map(new RichMapFunction[String, (String, Long)] {
//        override def map(value: String): (String, Long) = {
//          val splits = value.split(" ")
//          (splits(0), splits(1).toLong)
//        }
//      })
//      .keyBy(0)
//      .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(10)))
//      .trigger(new CustomProcessTimeTrigger)
//      .fold("", new FoldFunction[(String, Long), String] {
//        override def fold(accumulator: String, value: (String, Long)): String = {
//          accumulator + value._2
//        }
//      })
//
//    fold.print()
//    env.execute("TumblingWindowsFold")
//
//  }
//
//}
