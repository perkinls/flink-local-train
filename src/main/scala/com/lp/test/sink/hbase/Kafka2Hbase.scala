package com.lp.test.sink.hbase

import com.lp.test.utils.ConfigUtils
import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.{CheckpointConfig, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes


/**
  * <p/> 
  * <li>Description: kafka数据源写入hbase数据库</li>
  * <li>@author: li.pan</li> 
  * <li>Date: 2019-06-14 13:55</li> 
  * <li>Version: V1.0</li> 
  * 代码仅供参考
  */
object Kafka2Hbase {
  def main(args: Array[String]): Unit = {

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
      .setStartFromEarliest()
    val dataStream = env.addSource(kafkaConsumer)
    dataStream.writeUsingOutputFormat(new HBaseOutputFormat)
  }

  @SerialVersionUID(99L)
  class HBaseOutputFormat extends OutputFormat[String] {
    var conf: org.apache.hadoop.conf.Configuration = _
    var table: HTable = _
    var taskNumber: String = _
    var rowNumber: Int = _

    override def configure(parameters: Configuration): Unit = {

      conf = HBaseConfiguration.create
    }

    override def open(taskNumber: Int, numTasks: Int): Unit = {
      table = new HTable(conf, "flinkExample")
      this.taskNumber = String.valueOf(taskNumber)

    }

    override def writeRecord(record: String): Unit = {
      val put = new Put(Bytes.toBytes(taskNumber + rowNumber))
      put.add(Bytes.toBytes("entry"), Bytes.toBytes("entry"), Bytes.toBytes(rowNumber))
      rowNumber += 1
      table.put(put)
    }

    override def close(): Unit = {
      table.flushCommits()
      table.close()
    }
  }

}
