package com.lp.test.watermark

import java.util.Properties

import com.lp.test.serialization.KafkaEventSchema
import com.lp.test.utils.ConfigUtils
import net.sf.json.JSONObject
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
  * <p/> 
  * <li>Description: 设置自定义时间戳分配器和watermark发射器</li>
  * <li>@author: lipan@cechealth.cn</li> 
  * <li>Date: 2019-05-08 22:09</li> 
  */
object KafkaSourceWatermarkTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.getConfig.setAutoWatermarkInterval(1000) //设置周期性watermark生成时间

    //设置事件事件
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val kafkaConfig = ConfigUtils.apply("json")

    val kafkaConsumer = new FlinkKafkaConsumer(kafkaConfig._1, new KafkaEventSchema, kafkaConfig._2)
      .setStartFromEarliest()
      .assignTimestampsAndWatermarks(new CustomWatermarkExtractor) //设置自定义时间戳分配器和watermark发射器，也可以在后面的算子中设置


    import org.apache.flink.api.scala._
    env
      .addSource(kafkaConsumer)
      //      .assignTimestampsAndWatermarks(CustomWatermarkExtractor)//设置自定义时间戳分配器和watermark发射器
      .keyBy(_.getString("fruit"))
      .window(TumblingEventTimeWindows.of(Time.seconds(10))) //滚动窗口，大小为10s
      .allowedLateness(Time.seconds(10)) //允许10秒延迟
      .reduce(new ReduceFunction[JSONObject] { //对json字符串中key相同的进行聚合操作
      override def reduce(value1: JSONObject, value2: JSONObject): JSONObject = {
        val json = new JSONObject()
        json.put("fruit", value1.getString("fruit"))
        json.put("number", value1.getInt("number") + value2.getInt("number"))
        json
      }
    }).print()

    env.execute("KafkaSourceWatermarkTest")

  }

}
