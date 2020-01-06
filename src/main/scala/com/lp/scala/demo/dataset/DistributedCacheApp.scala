package com.lp.scala.demo.dataset

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

/**
  * <p/>
  * <li>title: DataSet 分布式缓存</li>
  * <li>@author: li.pan</li>
  * <li>Date: 2019/11/23 2:15 下午</li>
  * <li>Version: V1.0</li>
  * <li>Description:
  * step1: 注册一个本地/HDFS文件
  * step2：在open方法中获取到分布式缓存的内容即可
  * </li>
  */

object DistributedCacheApp {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val filePath = "file:///Users/lipan/workspace/flink_demo/flink-local-train/src/main/resources/sink/scala/cache.txt"

    // step1: 注册一个本地/HDFS文件
    env.registerCachedFile(filePath, "pk-scala-dc")

    import org.apache.flink.api.scala._
    val data = env.fromElements("hadoop", "spark", "flink", "pyspark", "storm")

    data.map(new RichMapFunction[String, String] {

      // step2：在open方法中获取到分布式缓存的内容即可
      override def open(parameters: Configuration): Unit = {
        val dcFile = getRuntimeContext.getDistributedCache().getFile("pk-scala-dc")

        val lines = FileUtils.readLines(dcFile) // java

        /**
          * 此时会出现一个异常：java集合和scala集合不兼容的问题
          */
        import scala.collection.JavaConverters._
        for (ele <- lines.asScala) {
          println(ele)
        }
      }

      override def map(value: String): String = value
    }).print()
  }

}
