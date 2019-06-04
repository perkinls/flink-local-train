package com.lp.test.dataset

import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
  * <p/> 
  * <li>Description: flink中计数器的实现</li>
  * <li>@author: lipan@cechealth.cn</li> 
  * <li>Date: 2019-04-14 21:53</li> 
  */
object CountApp {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = env.fromElements("hadoop","spark","flink","pyspark","storm")


    val info = data.map(new RichMapFunction[String,String]() {

      // step1：定义计数器
      val counter = new LongCounter()

      override def open(parameters: Configuration): Unit = {
        // step2: 注册计数器
        getRuntimeContext.addAccumulator("ele-counts-scala", counter)
      }

      override def map(in: String): String = {
        counter.add(1)
        in
      }
    })

    val filePath = "file:///Users/lipan/workspace/flink_demo/flink-local-train/src/main/resources/sink"
    info.writeAsText(filePath, WriteMode.OVERWRITE).setParallelism(3)
    val jobResult = env.execute("CounterApp")

    // step3: 获取计数器
    val num = jobResult.getAccumulatorResult[Long]("ele-counts-scala")

    println("num: " + num)
  }

}
