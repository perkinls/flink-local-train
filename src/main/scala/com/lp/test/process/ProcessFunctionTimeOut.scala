package com.lp.test.process

import com.lp.test.utils.ConfigUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, ProcessFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

/**
  * <p/> 
  * <li>Description: TODO</li>
  * <li>@author: li.pan</li> 
  * <li>Date: 2019-06-14 20:43</li> 
  * <li>Version: V1.0</li> 
  * 侧输出和滚动窗口中使用过
  * 测试方法：
  * 先开启producer 跑一会儿，然后停掉producer，过一分钟以后启动，因为我们采用的时间是注入时间，所以过一分钟之后发数据
  * 事件时间大于1分钟，结果才会输出。
  * 也可以设计某一个key，每隔1.5分钟发一次，这样就可以看到结果输出了
  * 实际上该例子就是实现了会话窗口的功能。
  *
  * https://www.jianshu.com/p/e6297fac67cb
  *
  */
object ProcessFunctionTimeOut {
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

    //构建kafka消费者
    val kafkaConsumer = new FlinkKafkaConsumer(kafkaConfig._1, new SimpleStringSchema(), kafkaConfig._2)
      .setStartFromLatest()
      .assignTimestampsAndWatermarks(new CustomWatermarkExtractor) //设置自定义时间戳分配器和watermark发射器，也可以在后面的算子中设置


    import org.apache.flink.api.scala._
    val process = env
      .addSource(kafkaConsumer)
      .map(new RichMapFunction[String, (String, Long)] {
        override def map(value: String): (String, Long) = {
          val splits = value.split(" ")
          (splits(0), splits(1).toLong)
        }
      })
      .keyBy(0)
      .process(new CountWithTimeoutFunction)
    process.print()

    env.execute("ProcessFunctionTimeOut")

  }


  /**
    * todo
    */
  class CountWithTimeoutFunction extends ProcessFunction[(String, Long), (String, Long)] {

    //保留处理函数内部的所有状态 一个并行度
    var state: ValueState[CountWithTimestamp] = _

    override def open(parameters: Configuration): Unit = {
      state = getRuntimeContext.getState(new ValueStateDescriptor[CountWithTimestamp]("myState", classOf[CountWithTimestamp]))
    }

    /**
      * 针对每一个元素会调用该方法，根据相关逻辑去更改内部状态
      * （processElement其实是更新内部状态和注册TimerService）
      *
      * @param value
      * @param ctx 上下文(Context)对象,这个对象能访问到所处理元素事件时间的时间戳还有定时服务器(TimerService)
      *            为尚未发生的处理时间或事件时间实例注册回调函数。当一个定时器到达特定的时间实例时，onTimer(...)
      *            方法就会被调用。在这个函数的调用期间，所有的状态(states)都会再次对应定时器被创建时key所属的states，
      *            同时被触发的回调函数也能操作这些状态。
      * @param out
      */
    override def processElement(value: (String, Long),
                                ctx: ProcessFunction[(String, Long),
                                  (String, Long)]#Context,
                                out: Collector[(String, Long)]): Unit = {

      // retrieve the current count
      var current = state.value
      if (current == null) {
        current = new CountWithTimestamp()
        current.key = value._1
      }

      // update the state's count
      current.count += 1

      // set the state's timestamp to the record's assigned event time timestamp
      current.lastModified = ctx.timestamp

      // 对当前key进行状态更新
      state.update(current)

      // schedule the next timer 60 seconds from the current event time
      //注册基于事件时间的timer
      ctx.timerService.registerEventTimeTimer(current.lastModified + 60000)

      // 注册基于处理时间的timer
      ctx.timerService.registerProcessingTimeTimer(current.lastModified + 60000)

    }

    /**
      * 计时器，当计时器时间到超时时间到时会调用onTimer方法输出
      * （onTime的功能是输出逻辑判断）
      *
      * @param timestamp
      * @param ctx
      * @param out
      */
    override def onTimer(timestamp: Long, ctx: ProcessFunction[(String, Long), (String, Long)]#OnTimerContext, out: Collector[(String, Long)]): Unit = {

      // get the state for the key that scheduled the timer
      val result = state.value

      System.out.println("onTimer : " + result.key)
      // check if this is an outdated timer or the latest timer
      if (timestamp == result.lastModified + 60000) {
        System.out.println("onTimer timeout : " + result.key)

        // emit the state on timeout
        out.collect((result.key, result.count))
      }
    }
  }

  /**
    * 该自定义时间戳抽取器和触发器实际上是使用的注入时间，因为发的数据不带时间戳的。
    *
    */
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

      currentTimestamp = System.currentTimeMillis()
      currentTimestamp
    }
  }


}
