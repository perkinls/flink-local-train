package com.lp.test.source

import java.util
import java.util.Properties

import com.lp.test.trigger.CustomProcessTimeTrigger
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.jobgraph.JobVertex
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer


/**
  * <p/> 
  * <li>Description: kafka消费者测试</li>
  * <li>@author: lipan@cechealth.cn</li> 
  * <li>Date: 2019-05-07 22:31</li> 
  */
object KafkaSourceTest {

  def main(args: Array[String]): Unit = {

    //获取flink流式运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置最少处理一次语义和恰好一次语义
    env.enableCheckpointing(20000, CheckpointingMode.AT_LEAST_ONCE)
    //checkpointing可以分开设置
    //env.enableCheckpointing(20000)
    //env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    //设置checkpoint目录
    //env.setStateBackend(new FsStateBackend("/hdfs/checkpoint"));

    //设置checkpoint的清除策略
    env.getCheckpointConfig.enableExternalizedCheckpoints(
      CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    //设置重启策略
    env.setRestartStrategy(RestartStrategies.
      fixedDelayRestart(5, //5次尝试
        50000)) //每次重试间隔50s

    //设置flink以身为时间为基准作，处理事件
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val props = new Properties()
    props.setProperty("bootstrap.servers", "master:9092")
    props.setProperty("group.id", "test")

    val kafkaConsumer = new FlinkKafkaConsumer("fk_string_topic",
      new SimpleStringSchema(),
      props)
    kafkaConsumer.setStartFromEarliest()

    import org.apache.flink.api.scala._
    val stream = env
      .addSource(kafkaConsumer)
      .map(new RichMapFunction[String, Int] {
        override def map(value: String): Int = {
          Integer.valueOf(value)
        }
      })
      .timeWindowAll(Time.seconds(20))
      .trigger(new CustomProcessTimeTrigger)

    stream.sum(0).print()

//    val vertices: util.Iterator[JobVertex] = env.getStreamGraph.getJobGraph().getVertices.iterator()
//    while (vertices.hasNext) {
//      val value = vertices.next()
//      println("NAME======>" + value.getName)
//      println("ID======>" + value.getID)
//    }
    env.execute("KafkaSourceTest")


  }

}
