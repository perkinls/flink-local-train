package com.lp.test.join

import java.util.Properties

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

/**
  * <p/> 
  * <li>Description: 区间关联Join</li>
  * <li>@author: lipan@cechealth.cn</li> 
  * <li>Date: 2019-05-08 21:02</li> 
  */
object IntervalJoin {

  def main(args: Array[String]): Unit = {

    //获取flink运行时环境
    val env=StreamExecutionEnvironment.getExecutionEnvironment

    //设置至少一次或仅此一次语义
    env.enableCheckpointing(20000,CheckpointingMode.EXACTLY_ONCE)

    //设置checkpoint目录
    env.getCheckpointConfig.enableExternalizedCheckpoints(
      CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    //设置重启策略
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5,
      50000))

    //选择设置事件时间和处理事件
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val props = new Properties()
    props.setProperty("bootstrap.servers", "master:9092")
    props.setProperty("group.id", "TumblingWindowJoin")
    val kafkaConsumer1=new FlinkKafkaConsumer("fk_kv_topic",new SimpleStringSchema(),props)
    kafkaConsumer1.setStartFromLatest()

    val kafkaConsumer2=new FlinkKafkaConsumer("fk_kv_1_topic",new SimpleStringSchema(),props)
    kafkaConsumer2.setStartFromLatest()

    import org.apache.flink.api.scala._
    val operator1 = env.addSource(kafkaConsumer1)
      .map(new RichMapFunction[String, (String, Long)] { //对kafka中的数据进行转换
        override def map(value: String): (String, Long) = {
          val splits = value.split(" ")
          (splits(0), splits(1).toLong)
        }
      })

    val operator2 = env.addSource(kafkaConsumer1)
      .map(new RichMapFunction[String, (String, Long)] { //对kafka中的数据进行转换
        override def map(value: String): (String, Long) = {
          val splits = value.split(" ")
          (splits(0), splits(1).toLong)
        }
      })
    operator1
      .keyBy(0)
      .intervalJoin(operator2.keyBy(0))
      .between(Time.milliseconds(-2),Time.milliseconds(1))
      .process(new ProcessJoinFunction[(String,Long),(String,Long),String] {
        override def processElement(left: (String, Long), right: (String, Long), ctx: ProcessJoinFunction[(String, Long), (String, Long), String]#Context, out: Collector[String]): Unit = {
          out.collect(null)
        }
      }).print()
    env.execute("IntervalJoin")

  }



}
