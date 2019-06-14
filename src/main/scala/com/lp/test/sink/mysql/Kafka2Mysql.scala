package com.lp.test.sink.mysql

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.lp.test.trigger.CustomProcessTimeTrigger
import com.lp.test.utils.ConfigUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{AllWindowedStream, DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
  * <p/> 
  * <li>Description: kafka数据源写入mysql数据库</li>
  * <li>@author: li.pan</li> 
  * <li>Date: 2019-06-14 13:12</li> 
  * <li>Version: V1.0</li> 
  *  需要手动建表（不建表,未报错）
  */
object Kafka2Mysql {
  def main(args: Array[String]): Unit = {

    //获取flink流式运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置最少处理一次语义和恰好一次语义
    env.enableCheckpointing(20000, CheckpointingMode.AT_LEAST_ONCE)

    //checkpointing可以分开设置
    //env.enableCheckpointing(20000)
    //env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    //设置checkpoint目录
    //env.setStateBackend(new FsStateBackend("/hdfs/checkpoint"));
    //设置checkpoint的清除策略
    env.getCheckpointConfig.enableExternalizedCheckpoints(
      CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    //设置重启策略
    env.setRestartStrategy(RestartStrategies.
      fixedDelayRestart(5, //5次尝试
        50000)) //每次重试间隔50s

    //设置flink以身为时间为基准作，处理事件
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val kafkaConfig = ConfigUtils.apply("string")

    val kafkaConsumer = new FlinkKafkaConsumer(kafkaConfig._1, new SimpleStringSchema(), kafkaConfig._2)
      .setStartFromLatest()

    import org.apache.flink.api.scala._
    val windowStream: AllWindowedStream[Int, TimeWindow] = env
      .addSource(kafkaConsumer)
      .map(new RichMapFunction[String, Int] {
        override def map(value: String): Int = {
          Integer.valueOf(value)
        }
      })
      .timeWindowAll(Time.seconds(20))
      .trigger(new CustomProcessTimeTrigger)
    windowStream.min(0).print("1234")

    val sum: DataStream[Int] = windowStream.sum(0)

    sum.print("xxxxxxx")
    sum.addSink(new CustomJdbcSink)

    env.execute("Kafka2Mysql")
  }

  /**
    * 自定义sink写入mysql
    */
  class CustomJdbcSink extends RichSinkFunction[Int] {

    var conn: Connection = _
    var ps: PreparedStatement = _

    /**
      * open方法是初始化方法，会在invoke方法之前执行，执行一次。
      */
    @throws
    override def open(parameters: Configuration): Unit = {
      println("调用open方法")
      //随便写一张表
      Class.forName("com.mysql.jdbc.Driver")
      conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "1234")
      val sql = s"insert into kafka_sum(total) values (?)"

      println(s"$conn 执行mysql写操作")
      ps = conn.prepareStatement(sql)
      super.open(parameters)
    }

    @throws
    override def invoke(value: Int, context: SinkFunction.Context[_]): Unit = {
      try {
        ps.setInt(1, value)
        ps.executeUpdate()
        println("执行完成")
      } catch {
        case e: Exception => e.toString
      }

    }

    @throws
    override def close(): Unit = {
      if (ps != null)
        ps.close()
      if (conn != null)
        conn.close()
      super.close()
    }
  }

}
