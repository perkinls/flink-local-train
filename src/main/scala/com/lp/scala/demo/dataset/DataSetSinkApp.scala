package com.lp.scala.demo.dataset

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
  * <p/> 
  * <li>Description: Sink</li>
  * <li>@author: panli0226@sina.com</li> 
  * <li>Date: 2019-04-14 21:33</li> 
  */
object DataSetSinkApp {

  def main(args: Array[String]): Unit = {

    val env=ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val data=1 to 10
    val text=env.fromCollection(data)

    val filePath="file:///Users/lipan/workspace/flink_demo/flink-local-train/src/main/resources/sink/scala/sink_test.txt"
    //可以写的任何文件系统
    text.writeAsText(filePath,WriteMode.OVERWRITE)

    env.execute("SinkApp")
  }
}
