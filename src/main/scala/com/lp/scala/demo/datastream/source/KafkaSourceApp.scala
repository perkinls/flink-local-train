package com.lp.scala.demo.datastream.source

import com.lp.scala.demo.datastream.trigger.CustomProcessTimeTrigger
import com.lp.scala.demo.utils.ConfigUtils
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
  * <li>Description: kafka消费者测试,自定义触发器</li>
  * <li>@author: panli0226@sina.com</li> 
  * <li>Date: 2019-05-07 22:31</li> 
  */
object KafkaSourceApp {

  def main(args: Array[String]): Unit = {

    //获取flink流式运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置最少处理一次语义和恰好一次语义
    env.enableCheckpointing(20000, CheckpointingMode.AT_LEAST_ONCE)

    //checkpointing可以分开设置
    //env.enableCheckpointing(20000)
    //env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    //设置checkpoint目录 state存储路径

    //env.setStateBackend(new FsStateBackend("/hdfs/checkpoint"));
    //设置checkpoint的清除策略
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    /**
      * 设置重启策略/5次尝试/每次重试间隔50s
      */
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 50000))

    //设置flink以身为时间为基准作，处理事件
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val kafkaConfig = ConfigUtils.apply("string")

    val kafkaConsumer = new FlinkKafkaConsumer(kafkaConfig._1, new SimpleStringSchema(), kafkaConfig._2)
      .setStartFromLatest()

    import org.apache.flink.api.scala._

    val stream = env
      .addSource(kafkaConsumer)
      .setParallelism(1)
      .map(new RichMapFunction[String, Int] {
        override def map(value: String): Int = {
          Integer.valueOf(value)
        }
      })
      .timeWindowAll(Time.seconds(20))
      .trigger(new CustomProcessTimeTrigger) //10个元素触发一次计算

    stream.sum(0).print()

    // 获取JobGraph
//    val vertices = env.getStreamGraph.getJobGraph.getVertices
//    import scala.collection.JavaConversions._
//    for (vertex <- vertices) {
//      System.out.println("=====>" + vertex.getName)
//      System.out.println("=====>" + vertex.getID)
//    }
    env.execute("KafkaSourceTest")


  }

}
