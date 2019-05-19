package com.lp.test.dataset

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
  * <p/> 
  * <li>Description: TODO</li>
  * <li>@author: lipan@cechealth.cn</li> 
  * <li>Date: 2019-04-14 21:33</li> 
  */
object DataSetSinkApp {

  def main(args: Array[String]): Unit = {

    val env=ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val data=1 to 10
    val text=env.fromCollection(data)

    //可以写的任何文件系统
    text.writeAsText("",WriteMode.OVERWRITE)

    env.execute("SinkApp")
  }
}
