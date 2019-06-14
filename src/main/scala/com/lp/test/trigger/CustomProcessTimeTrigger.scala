package com.lp.test.trigger

import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
  * <p/> 
  * <li>Description: 自定义窗口触发器</li>
  * 注意：⚠️
  * 1. onElement() onEventTime() onProcessingTime()决定着如何通过返回一个TriggerResult来操作输入事件
  * CONTINUE:什么都不做。
  * FIRE:触发计算。
  * PURE:清除窗口的元素。
  * FIRE_AND_PURE:触发计算和清除窗口元素。
  * 2. 这些方法中的任何一个都可用于为将来的操作注册处理或事件时间计时器
  *
  * Fire 和 Purge
  * 当Trigger确定窗口可以被处理的时候，就会触发处理，操作简单仅仅需要返回FIRE或者FIRE_AND_PURGE即可，这是windows要输出结果的信号。
  * 当trigger触发执行输出的时候，有两个选择：FIRE 或者 FIRE_AND_PURGE。FIRE会保留window的内容，而FIRE_AND_PURGE会删除其内容。
  * 默认情况下，预先实现的trigger仅仅是FIRE，不会清除窗口的状态。
  * 注意purgeing将仅仅删除窗口的内容，并将完好保留有关窗口和任何触发状态的所有元信息。
  *
  * <li>@author: lipan@cechealth.cn</li> 
  * <li>Date: 2019-05-07 22:55</li> 
  */
class CustomProcessTimeTrigger extends Trigger[Int, TimeWindow] {

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
  override def onElement(element: Int, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {

    //注册系统时间回调。当前系统时间超过指定时间
    ctx.registerProcessingTimeTimer(window.maxTimestamp)
    // CONTINUE是代表不做输出，也就是，此时我们想要实现比如10条输出一次，
    // 而不是窗口结束再输出就可以在这里实现。
    if (flag > 9) {
      System.out.println("onElement : " + flag + "触发计算，保留Window内容")
      flag = 0
      TriggerResult.FIRE
    }
    else flag += 1
//    System.out.println("onElement : " + element)
    TriggerResult.CONTINUE
  }

  /**
    * 处理事件time出发的时候会被调用
    *
    * @param time
    * @param window
    * @param ctx
    * @return
    */
  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE

  /**
    * 窗口事件时间time end的时候被调用
    *
    * @param time
    * @param window
    * @param ctx
    * @return
    */
  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE

  /**
    * 如果此触发器支持合并触发器状态，则返回true
    *
    * @return
    */
  override def canMerge() = {
    true
  }

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext) = ctx.deleteProcessingTimeTimer(window.maxTimestamp)

  /**
    * 有状态的触发器相关，并在他们相应的窗口合并时合并两个触发器的状态，例如使用会话窗口
    *
    * @param window
    * @param ctx
    */
  override def onMerge(window: TimeWindow, ctx: Trigger.OnMergeContext): Unit = {
    // only register a timer if the time is not yet past the end of the merged window
    // this is in line with the logic in onElement(). If the time is past the end of
    // the window onElement() will fire and setting a timer here would fire the window twice.
    val windowMaxTimestamp = window.maxTimestamp
    if (windowMaxTimestamp > ctx.getCurrentProcessingTime) ctx.registerProcessingTimeTimer(windowMaxTimestamp)
  }

  override def toString: String = "ProcessingTimeTrigger()"


}
