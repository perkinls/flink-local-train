package com.lp.test.datastream

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * <p/> 
  * <li>Description: TODO</li>
  * <li>@author: lipan@cechealth.cn</li> 
  * <li>Date: 2019-04-15 12:55</li> 
  */
object DataStreamWordCountApp {

  def main(args: Array[String]): Unit = {

    val env=StreamExecutionEnvironment.getExecutionEnvironment
    val textDataStream: DataStream[String] = env.socketTextStream("localhost",9999)

    import org.apache.flink.api.scala._
    val counts = textDataStream.flatMap { _.toLowerCase.split(" ") filter { _.nonEmpty } }
      .map { (_, 1) }
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)

    counts.print()

    env.execute("Window Stream WordCount")
  }

}
