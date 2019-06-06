package com.lp.test.join

import com.lp.test.utils.ConfigUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

/**
  * <p/> 
  * <li>Description: 区间关联Join</li>
  * https://blog.csdn.net/xxscj/article/details/85622418
  * "区间关联当前仅支持EventTime"
  * Interval JOIN 相对于UnBounded的双流JOIN来说是Bounded JOIN。就是每条流的每一条数据会与另一条流上的不同时间区域的数据进行JOIN。
  * <li>@author: lipan@cechealth.cn</li> 
  * <li>Date: 2019-05-08 21:02</li> 
  */
object IntervalJoin {

  def main(args: Array[String]): Unit = {

    //获取flink运行时环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置至少一次或仅此一次语义
    env.enableCheckpointing(20000, CheckpointingMode.EXACTLY_ONCE)

    //设置checkpoint目录
    env.getCheckpointConfig.enableExternalizedCheckpoints(
      CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    //设置重启策略
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5,
      50000))

    //选择设置事件时间和处理事件
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val kafkaConfig = ConfigUtils.apply("kv")
    val kafkaConfig1 = ConfigUtils.apply("kv1")

    //构建kafka消费者
    val kafkaConsumer1 = new FlinkKafkaConsumer(kafkaConfig._1, new SimpleStringSchema(), kafkaConfig._2)
      .setStartFromLatest()
      .assignTimestampsAndWatermarks(new CustomWatermarkExtractor) //设置自定义时间戳分配器和watermark发射器，也可以在后面的算子中设置


    val kafkaConsumer2 = new FlinkKafkaConsumer(kafkaConfig1._1, new SimpleStringSchema(), kafkaConfig1._2)
      .setStartFromLatest()
      .assignTimestampsAndWatermarks(new CustomWatermarkExtractor)


    import org.apache.flink.api.scala._
    val operator1 = env.addSource(kafkaConsumer1)
      .map(new RichMapFunction[String, (String, Long)] { //对kafka中的数据进行转换
        override def map(value: String): (String, Long) = {
          val splits = value.split(" ")
          (splits(0), splits(1).toLong)
        }
      })

    val operator2 = env.addSource(kafkaConsumer2)
      .map(new RichMapFunction[String, (String, Long)] { //对kafka中的数据进行转换
        override def map(value: String): (String, Long) = {
          val splits = value.split(" ")
          (splits(0), splits(1).toLong)
        }
      })
    operator1
      .keyBy(0)
      .intervalJoin(operator2.keyBy(0))
      .between(Time.milliseconds(-2), Time.milliseconds(1))
      .process(new ProcessJoinFunction[(String, Long), (String, Long), String] {
        override def processElement(left: (String, Long), right: (String, Long), ctx: ProcessJoinFunction[(String, Long), (String, Long), String]#Context, out: Collector[String]): Unit = {
          out.collect(null)
        }
      }).print()
    env.execute("IntervalJoin")

  }

  class CustomWatermarkExtractor extends AssignerWithPeriodicWatermarks[String] {

    var currentTimestamp = Long.MinValue

    /**
      * waterMark生成器
      *
      * @return
      */
    override def getCurrentWatermark: Watermark = {

      new Watermark(
        if (currentTimestamp == Long.MinValue)
          Long.MinValue
        else
          currentTimestamp - 1
      )
    }

    /**
      * 时间抽取
      *
      * @param element
      * @param previousElementTimestamp
      * @return
      */
    override def extractTimestamp(element: String, previousElementTimestamp: Long): Long = {

      val strings = element.split(" ")
      this.currentTimestamp = strings(2).toLong

      currentTimestamp
    }
  }

}
