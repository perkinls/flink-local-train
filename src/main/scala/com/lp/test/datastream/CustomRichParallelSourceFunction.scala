package com.lp.test.datastream

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

/**
  * <p/> 
  * <li>Description: TODO</li>
  * <li>@author: lipan@cechealth.cn</li> 
  * <li>Date: 2019-04-15 20:25</li> 
  */
class CustomRichParallelSourceFunction extends RichParallelSourceFunction[Long]{

  var count = 1L

  var isRunning = true

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
