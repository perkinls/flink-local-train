package com.lp.test.sink.redis

import com.lp.test.trigger.CustomProcessTimeTrigger
import com.lp.test.utils.ConfigUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.{AllWindowedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
  * <p/> 
  * <li>Description: kafka数据源写入redis</li>
  * <li>@author: li.pan</li> 
  * <li>Date: 2019-06-14 13:44</li> 
  * <li>Version: V1.0</li> 
  * redis中会自动新建key
  */
object Kafka2Redis {

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
    val jedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build()

    windowStream.sum(0).addSink(new RedisSink[Int](jedisPoolConfig, new RedisExampleMapper))
    env.execute("Kafka2Mysql")
  }

  class RedisExampleMapper extends RedisMapper[Int] {
    /**
      * 设置数据使用的数据结构 Set
      *
      * @return
      */
    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.SET)
    }

    /**
      * 指定key
      *
      * @param t
      * @return
      */
    override def getKeyFromData(t: Int): String = {
      "kafka2redis"
    }

    /**
      * 指定value
      *
      * @param t
      * @return
      */
    override def getValueFromData(t: Int): String = {
      t.toString
    }
  }

}
