package com.lp.test.windows

import com.lp.test.utils.ConfigUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

/**
  * <p/> 
  * <li>Description: 统计一个window中元素个数，此外，还将window的信息添加到输出中。</li>
  * 使用ProcessWindowFunction来做简单的聚合操作，如:计数操作，性能是相当差的。
  * 将ReduceFunction跟ProcessWindowFunction结合起来，来获取增量聚合和添加到ProcessWindowFunction中的信息，性能更好
  * <li>@author: lipan@cechealth.cn</li> 
  * <li>Date: 2019-05-10 16:35</li> 
  */
object TumblingWindowsProcessFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.getConfig.setAutoWatermarkInterval(1000) //watermark间隔时间

    //设置事件事件
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val kafkaConfig = ConfigUtils.apply("kv")

    val kafkaConsumer = new FlinkKafkaConsumer(kafkaConfig._1,
      new SimpleStringSchema(),
      kafkaConfig._2).setStartFromLatest()

    import org.apache.flink.api.scala._
    val process = env
      .addSource(kafkaConsumer)
      .map(new RichMapFunction[String, (String, Long)] {
        override def map(value: String): (String, Long) = {
          val splits = value.split(" ")
          (splits(0), splits(1).toLong)
        }
      })
      .keyBy(_._1)
      .timeWindow(Time.minutes(5)) //滚动窗口
      .trigger(new CustomProcessTimeTrigger)
      .process(new MyProcessWindowFunction)

    process.print()
    env.execute("TumblingWindowsProcessFunction")
  }

  /**
    * 注意ProcessWindowFunction的包！！！！！
    *
    * 获取包含窗口的所有数据元的Iterable，以及可访问时间和状态信息的Context对象，这使其能够提供比其他窗口函数更多的灵活性。
    * 这是以性能和资源消耗为代价的，因为数据元不能以递增方式聚合，而是需要在内部进行缓冲，直到窗口被认为已准备好进行处理。
    *
    * ProcessWindowFunction可以与ReduceFunction，AggregateFunction或FoldFunction以递增地聚合数据元。
    * 当窗口关闭时，ProcessWindowFunction将提供聚合结果。这允许它在访问附加窗口元信息的同时递增地计算窗口ProcessWindowFunction。
    *
    */
  class MyProcessWindowFunction extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {

    def process(key: String, context: Context, input: Iterable[(String, Long)], out: Collector[String]) = {
      var count = 0L
      //ProcessWindowFunction对窗口中的数据元进行计数的情况
      for (in <- input)
        count = count + 1

      out.collect(s"Window ${context.window} count: $count")
    }
  }

  /**
    * 自定义触发器
    *
    * CONTINUE： 没做什么，
    * FIRE：触发​​计算，
    * PURGE：清除窗口中的数据元和
    * FIRE_AND_PURGE：触发​​计算并清除窗口中的数据元。
    */
  class CustomProcessTimeTrigger extends Trigger[(String, Long), TimeWindow] {

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
    override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {

      //注册系统时间回调。当前系统时间超过指定时间
      ctx.registerEventTimeTimer(window.maxTimestamp)
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

}
