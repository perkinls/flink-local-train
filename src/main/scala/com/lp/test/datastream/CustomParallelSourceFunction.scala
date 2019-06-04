package com.lp.test.datastream

import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}

/**
  * <p/> 
  * <li>Description: 自定义source能够并行</li>
  * <li>@author: lipan@cechealth.cn</li> 
  * <li>Date: 2019-04-15 20:05</li> 
  */
class CustomParallelSourceFunction extends ParallelSourceFunction[Long] {

  var isRunning = true
  var count = 1L

  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while (isRunning) {
      ctx.collect(count)
      count += 1
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {

    isRunning = false
  }
}
