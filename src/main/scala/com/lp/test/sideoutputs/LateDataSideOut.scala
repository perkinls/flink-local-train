package com.lp.test.sideoutputs

import com.lp.test.serialization.KafkaEventSchema
import com.lp.test.utils.ConfigUtils
import net.sf.json.JSONObject
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer


/**
  * <p/> 
  * <li>Description: 侧输出解决延迟数据</li>
  * <li>@author: lipan@cechealth.cn</li> 
  * <li>Date: 2019-05-10 13:48</li> 
  */
object LateDataSideOut {

  def main(args: Array[String]): Unit = {

    import org.apache.flink.api.scala._
    val lateOutputTag = new OutputTag[JSONObject]("late-data")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //选择设置事件时间和处理事件
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val kafkaConfig = ConfigUtils.apply("json")

    val kafkaConsumer = new FlinkKafkaConsumer(kafkaConfig._1,
      new KafkaEventSchema(), //自定义反序列化器
      kafkaConfig._2)
      .setStartFromLatest()
      .assignTimestampsAndWatermarks(new CustomWatermarkExtractor) //设置自定义时间戳分配器和watermark发射器

    import org.apache.flink.api.scala._
    val reduce = env
      .addSource(kafkaConsumer)
      .keyBy(_.getString("name"))
      .window(TumblingEventTimeWindows.of(Time.seconds(10))) //滑动窗口，大小为10s
      .allowedLateness(Time.seconds(10)) //允许10s延迟
      .sideOutputLateData(lateOutputTag) //延迟数据侧输出
      .reduce(new ReduceFunction[JSONObject] {
      override def reduce(value1: JSONObject, value2: JSONObject): JSONObject = {
        val name = value1.getString("name")
        val num1 = value1.getInt("number")
        val num2 = value2.getInt("number")
        val jsonStr = new JSONObject()
        jsonStr.put("name", name)
        jsonStr.put("number", num1 + num2)
        jsonStr
      }
    })

    reduce.print()
    reduce.getSideOutput(lateOutputTag).print()

    env.execute("LateDataSideOut")
  }

  class CustomWatermarkExtractor extends AssignerWithPeriodicWatermarks[JSONObject] {

    var currentTimestamp = Long.MinValue

    /**
      * waterMark生成器
      *
      * @return
      */
    override def getCurrentWatermark: Watermark = {

      new Watermark(
        if (currentTimestamp == Long.MinValue)
          Long.MinValue
        else
          currentTimestamp - 1
      )
    }
    /**
      * 时间抽取
      *
      * @param element
      * @param previousElementTimestamp
      * @return
      */
    override def extractTimestamp(element: JSONObject, previousElementTimestamp: Long): Long = {

      this.currentTimestamp = element.getLong("time")

      currentTimestamp
    }
  }


}
