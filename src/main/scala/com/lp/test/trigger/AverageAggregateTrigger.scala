package com.lp.test.trigger

import org.apache.flink.api.common.functions.AggregateFunction

/**
  * <p/> 
  * <li>Description: 累加器窗口触发器</li>
  * <li>@author: lipan@cechealth.cn</li> 
  * <li>Date: 2019-05-10 17:58</li> 
  */
class AverageAggregateTrigger extends AggregateFunction[(String, Long), (Long, Long), Double] {
  /**
    * 创建一个新的累加器，启动一个新的聚合
    *
    * @return
    */
  override def createAccumulator(): (Long, Long) = {
    (0, 0)
  }

  /**
    * 将给定的输入值添加到给定的累加器，返回 new accumulator值
    *
    * @param value
    * @param accumulator
    * @return
    */
  override def add(value: (String, Long), accumulator: (Long, Long)): (Long, Long) = {
    (accumulator._1 + value._1.toLong, accumulator._2 + 1)
  }

  /**
    * 从累加器中获取结果
    *
    * @param accumulator
    * @return
    */
  override def getResult(accumulator: (Long, Long)): Double = accumulator._1.asInstanceOf[Double] / accumulator._2

  /**
    * 合并两个累加器，返回一个新的累加器
    *
    * @param a
    * @param b
    * @return
    */
  override def merge(a: (Long, Long), b: (Long, Long)): (Long, Long) = {
    (a._1 + b._1, a._2 + b._2)
  }
}
