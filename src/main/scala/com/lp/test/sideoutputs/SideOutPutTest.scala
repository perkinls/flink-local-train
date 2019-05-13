package com.lp.test.sideoutputs

import java.util.Properties

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.{Collector, OutputTag}

/**
  * <p/> 
  * <li>Description: flink feature sideout demo</li>
  * flink的侧输出提供了这个功能，侧输出的输出类型可以与主流不同，可以有多个侧输出(sideoutput)，每个侧输出不同的类型。
  *     1. 定义OutputTag
  *     2. 使用特定的函数ProcessFunction、CoProcessFunction、ProcessWindowFunction、ProcessAllWindowFunction
  * <li>@author: lipan@cechealth.cn</li> 
  * <li>Date: 2019-05-10 13:28</li> 
  */
object SideOutPutTest {

  val outputTag = new OutputTag("side_output>5")

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //选择设置事件时间和处理事件
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val props = new Properties()
    props.setProperty("bootstrap.servers", "master:9092")
    props.setProperty("group.id", "test")

    val kafkaConsumer = new FlinkKafkaConsumer("side", new SimpleStringSchema(), props)
    kafkaConsumer.setStartFromEarliest()

    import org.apache.flink.api.scala._
    val mainStream = env.addSource(kafkaConsumer)
      .map(new RichMapFunction[String, Integer] {
        override def map(value: String): Integer = {
          Integer.valueOf(value)
        }
      })
      .process(new ProcessFunction[Integer, Integer] {
        override def processElement(value: Integer, ctx: ProcessFunction[Integer, Integer]#Context, out: Collector[Integer]): Unit = {
          if (value <= 5)
            out.collect(value) //小于5的数据正常输出
          else
            ctx.output(outputTag, "sideout-" + String.valueOf(value)) //侧输出，可能有多个
        }
      })
    mainStream.print()
    //    mainStream.getSideOutput(outputTag).print()
    env.execute("SideOutPutTest")
  }

}
