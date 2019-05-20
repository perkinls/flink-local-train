package com.lp.test.datastream

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
  * <p/> 
  * <li>Description: TODO</li>
  * <li>@author: lipan@cechealth.cn</li> 
  * <li>Date: 2019-04-15 21:34</li> 
  */
object DataStreamSinkToMysqlApp {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost", 9999)

    val personStream = text.map(new MapFunction[String, person] {
      override def map(value: String): person = {
        val spilt = value.split(",")

        person(Integer.parseInt(spilt(0)), spilt(1), Integer.parseInt(spilt(2)))
      }
    })
    personStream.addSink(new CustomSinkToMysql)

    env.execute("DataStreamSinkToMysqlApp")

  }


}
