package com.lp.test.join

import com.lp.test.utils.ConfigUtils
import org.apache.flink.api.common.functions.{JoinFunction, RichMapFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
  * <p/> 
  * <li>Description: window之间的join操作 </li>
  *
  * 注意：
  * join必须依赖窗口及watermark操作
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
    operator1.print("111")

    val operator2 = env.addSource(kafkaConsumer2)
      .map(new RichMapFunction[String, (String, Long)] { //对kafka中的数据进行转换
        override def map(value: String): (String, Long) = {
          val splits = value.split(" ")
          (splits(0), splits(1).toLong)
        }
      })
    operator2.print("222")

    val joinDs = operator1
      .join(operator2)
      .where(elem => elem._1)
      .equalTo(elem => elem._1)      //注意单位  注意单位！！！！！Time.minutes(1)
      .window(TumblingEventTimeWindows.of(Time.minutes(1))) //窗口分配器定义程序
      .apply(new JoinFunction[(String, Long), (String, Long), (String, Long, Long)] {
      override def join(first: (String, Long), second: (String, Long)): (String, Long, Long) = {
        (first._1, first._2, second._2)
      }
    })
//    joinDs.print("333")

    //设置触发器
    joinDs
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.milliseconds(5)))
      .trigger(new CustomProcessTimeTrigger)
      .reduce((e1, e2) => {
        (e1._1, e1._2 + e2._2, e1._3 + e2._3) //聚合操作
      }).print()

    env.execute("TumblingWindowJoin")

  }

  /**
    * 自定义触发器
    *
    * CONTINUE： 没做什么，
    * FIRE：触发​​计算，
    * PURGE：清除窗口中的数据元和
    * FIRE_AND_PURGE：触发​​计算并清除窗口中的数据元。
    */
  class CustomProcessTimeTrigger extends Trigger[(String, Long, Long), TimeWindow] {

    var flag = 0

    /**
      * 进入窗口的每个元素都会调用该方法
      *
      * @param element
      * @param timestamp
      * @param window
      * @param ctx
      * @return
      */
    override def onElement(element: (String, Long, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {

      //注册系统时间回调。当前系统时间超过指定时间
      ctx.registerProcessingTimeTimer(window.maxTimestamp)
      // CONTINUE是代表不做输出，也就是，此时我们想要实现比如10条输出一次，
      // 而不是窗口结束再输出就可以在这里实现。
      if (flag > 9) {
        System.out.println("onElement : " + flag + "触发计算，保留Window内容")
        flag = 0
        TriggerResult.FIRE
      }
      else
        flag += 1
      System.out.println("onElement : " + element)
      TriggerResult.CONTINUE
    }

    /**
      * 在注册的事件时间计时器触发时调用该方法。
      *
      * @param time
      * @param window
      * @param ctx
      * @return
      */
    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE

    /**
      * 在注册的处理时间计时器触发时调用该方法
      *
      * @param time
      * @param window
      * @param ctx
      * @return
      */
    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE

    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext) = ctx.deleteProcessingTimeTimer(window.maxTimestamp)

    /**
      * 如果此触发器支持合并触发器状态，则返回true
      *
      * @return
      */
    override def canMerge() = {
      true
    }

    /**
      * 该onMerge()方法与状态触发器相关，并且当它们的相应窗口合并时合并两个触发器的状态，例如当使用会话窗口时。
      *
      * @param window
      * @param ctx
      */
    override def onMerge(window: TimeWindow, ctx: Trigger.OnMergeContext): Unit = {

      val windowMaxTimestamp = window.maxTimestamp
      if (windowMaxTimestamp > ctx.getCurrentProcessingTime)
        ctx.registerProcessingTimeTimer(windowMaxTimestamp)
    }

    override def toString: String = "ProcessingTimeTrigger()"


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
