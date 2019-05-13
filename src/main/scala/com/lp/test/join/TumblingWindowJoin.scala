package com.lp.test.join

import java.io.Serializable
import java.util.{Properties, Random}

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
  * <p/> 
  * <li>Description: window之间的join操作 </li>
  * <li>@author: lipan@cechealth.cn</li> 
  * <li>Date: 2019-05-08 19:52</li> 
  */
object TumblingWindowJoin {
  def main(args: Array[String]): Unit = {

    //构建运行时环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置最少一次和恰一次处理语义
    env.enableCheckpointing(20000, CheckpointingMode.EXACTLY_ONCE)

    //设置checkpoint目录
    env.getCheckpointConfig.enableExternalizedCheckpoints(
      CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    //设置重启策略
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, //5次尝试
      50000)) //每次尝试间隔50s
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val props = new Properties()
    props.setProperty("bootstrap.servers", "master:9092")
    props.setProperty("group.id", "TumblingWindowJoin")

    //构建kafka消费者 293条
    val kafkaConsumer1 = new FlinkKafkaConsumer("fk_kv_topic", new SimpleStringSchema(), props)
    kafkaConsumer1.setStartFromEarliest()

    //73条
    val kafkaConsumer2 = new FlinkKafkaConsumer("fk_kv_1_topic", new SimpleStringSchema(), props)
    kafkaConsumer2.setStartFromEarliest()

    import org.apache.flink.api.scala._
    val operator1 = env.addSource(kafkaConsumer1)
      .map(new RichMapFunction[String, Example] { //对kafka中的数据进行转换
        override def map(value: String): Example = {
          val splits = value.split(" ")
          Example(splits(0), splits(1).toLong)
        }
      }) //.print()

    val operator2 = env.addSource(kafkaConsumer1)
      .map(new RichMapFunction[String, Example] { //对kafka中的数据进行转换
        override def map(value: String): Example = {
          val splits = value.split(" ")
          Example(splits(0), splits(1).toLong)
        }
      })


    operator1
      .join(operator2)
      .where(elem => elem.name)
      .equalTo(elem => elem.name)
      //todo 滚动窗口会产生笛卡尔积
      .window(TumblingEventTimeWindows.of(Time.milliseconds(10))) //窗口分配器定义程序
      .apply((e1, e2) => {
      (e1.name, e1.value, e2.value)
    }).print()

//      .keyBy(_._1)
//      .reduce((e1, e2) => {
//        (e1._1, e1._2 + e2._2, e1._3 + e2._3) //聚合操作
//      }).print()

    env.execute("TumblingWindowJoin")

  }

  case class Example(name: String, value: Long)

  class GradeSource(example: Example) extends Iterator[Example] with Serializable {

    def hasNext: Boolean = true

    def next: Example = {
      example
    }
  }


}
