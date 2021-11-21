package com.lp.scala.demo.quickstart

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * <p/>
  * <li>title: 批处理WordCount程序</li>
  * <li>@author: li.pan</li>
  * <li>Date: 2019/12/29 14:33 下午</li>
  * <li>Version: V1.0</li>
  * <li>Description: 使用Scala API来开发Flink的批处理应用程序.</li>
  */
object BatchWCScalaApp {


  def main(args: Array[String]): Unit = {

    val input = "file:///Users/lipan/workspace/flink_demo/flink-local-train/src/main/resources/data/input.txt"

    val env = ExecutionEnvironment.getExecutionEnvironment

    val text = env.readTextFile(input)

    // 引入隐式转换
    import org.apache.flink.api.scala._

    text.flatMap(_.toLowerCase.split("\t"))
      .filter(_.nonEmpty)
      .map((_,1))
      .groupBy(0)
      .sum(1).print()

  }

}
