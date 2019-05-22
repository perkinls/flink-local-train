package com.lp.test.table

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.TableEnvironment

/**
  * <p/> 
  * <li>Description: TODO</li>
  * <li>@author: lipan@cechealth.cn</li> 
  * <li>Date: 2019-04-16 12:50</li> 
  */
object TableSqlApiApp {

  def main(args: Array[String]): Unit = {

    //获取flink运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //获取flink table运行时环境
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    //获取DataStream数据流
    val text: DataStream[String] = env.socketTextStream("localhost", 9999)

    //将数据流转为Table对象
    val table = tableEnv.fromDataStream(text)
    //将数据流注册成一张表
    tableEnv.registerTable("person", table)

    val result = tableEnv.sqlQuery("")

//    tableEnv.toAppendStream(result).print()

    env.execute("TableSqlApiApp")
  }

  case class person(id: Int, name: String, age: Int)

}
