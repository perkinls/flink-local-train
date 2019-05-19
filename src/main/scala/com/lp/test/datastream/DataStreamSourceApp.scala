package com.lp.test.datastream

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}


/**
  * <p/> 
  * <li>Description: 流处理</li>
  * <li>@author: lipan@cechealth.cn</li> 
  * <li>Date: 2019-04-15 13:10</li> 
  */
object DataStreamSourceApp {


  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    socketStream(env)
    nonParallelSourceFunction(env)
    parallelSourceFunction(env)
    richParallelSourceFunction(env)

    env.execute("DataStreamSourceApp")

  }

  /**
    * 自定义source 可以设置并行处理
    *
    * @param env
    */
  def richParallelSourceFunction(env: StreamExecutionEnvironment) = {

    val data = env.addSource(new CustomParallelSourceFunction).setParallelism(2)
    data.print()
  }

  /**
    * 自定义source 可以设置并行处理
    *
    * @param env
    */
  def parallelSourceFunction(env: StreamExecutionEnvironment) = {

    val data = env.addSource(new CustomParallelSourceFunction).setParallelism(2)
    data.print()
  }

  /**
    * 自定义source 不能并行处理
    *
    * @param env
    */
  def nonParallelSourceFunction(env: StreamExecutionEnvironment) = {

    //data不能设置大于1的并行度
    val data = env.addSource(new CustomNonParallelSourceFunction)
    data.print()
  }

  /**
    * socket 简单流处理
    *
    * @param env
    */
  def socketStream(env: StreamExecutionEnvironment): Unit = {
    val textStream: DataStream[String] = env.socketTextStream("localhost", 9999)

    textStream.print().setParallelism(1)
  }

}
