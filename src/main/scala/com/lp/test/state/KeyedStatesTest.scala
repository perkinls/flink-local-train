//package com.lp.test.state
//
//import java.util.Properties
//
//import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
//import org.apache.flink.api.common.restartstrategy.RestartStrategies
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.environment.CheckpointConfig
//import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
//
///**
//  * <p/> 
//  * <li>Description: 带状态计算测试</li>
//  * <li>@author: lipan@cechealth.cn</li> 
//  * <li>Date: 2019-05-09 22:06</li> 
//  */
//object KeyedStatesTest {
//  def main(args: Array[String]): Unit = {
//
//    //获取flink运行时环境
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//
//    //设置至少一次或仅此一次语义
//    env.enableCheckpointing(20000, CheckpointingMode.EXACTLY_ONCE)
//
//    //设置checkpoint目录
//    env.getCheckpointConfig.enableExternalizedCheckpoints(
//      CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
//
//    //设置重启策略
//    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5,
//      50000))
//
//    //选择设置事件时间和处理事件
//    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
//    val props = new Properties()
//    props.setProperty("bootstrap.servers", "master:9092")
//    props.setProperty("group.id", "TumblingWindowJoin")
//
//    val kafkaConsumer = new FlinkKafkaConsumer[String]("fk_string_topic", SimpleStringSchema, props)
//
//    import org.apache.flink.api.scala._
//    env.addSource(kafkaConsumer).map(new RichMapFunction[String, (String, Long)] {
//
//      //todo 未实现
//
//      @transient lazy val stateValue: Long = _
//
//      override def open(parameters: Configuration): Unit = {
//        //        stateValue=getRuntimeContext.getReducingState()
//      }
//
//      override def map(value: String): (String, Long) = {
//        //        val count=
//        null
//      }
//    })
//
//
//  }
//
//}
