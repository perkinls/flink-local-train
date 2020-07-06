package com.lp.scala.demo.datastream.example

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.lp.scala.demo.common.person
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}


/**
  * <p/> 
  * <li>Description: 写入Mysql公用类别</li>
  * <li>@author: panli0226@sina.com</li> 
  * <li>Date: 2019-04-15 21:41</li> 
  */
class CustomSinkToMysql extends RichSinkFunction[person] {

  private[datastream] var connection: Connection = _
  private[datastream] var ps: PreparedStatement = _


  /**
    * 获取数据库连接
    */
  def getConnection() = {
    DriverManager.getConnection("jdbc:mysql://localhost:3306/flink_demo?user=root&password=1234")
  }

  /**
    * 在open方法中建立connection
    *
    * @param parameters
    * @throws Exception
    */
  @throws[Exception]
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    connection = getConnection
    val sql = "insert into student(id,name,age) values (?,?,?)"
    ps = connection.prepareStatement(sql)
    System.out.println("open")
  }


  /**
    * 每条记录插入时调用一次
    *
    * @param value
    * @param context
    * @throws Exception
    */
  @throws[Exception]
  override def invoke(value: person, context: SinkFunction.Context[_]): Unit = {
    System.out.println("invoke~~~~~~~~~")
    // 未前面的占位符赋值
    ps.setInt(1, value.id)
    ps.setString(2, value.name)
    ps.setInt(3, value.age)
    ps.executeUpdate
  }

  /**
    * 在close方法中要释放资源
    *
    * @throws Exception
    */
  @throws[Exception]
  override def close(): Unit = {
    super.close()
    if (ps != null) ps.close()
    if (connection != null) connection.close()
  }
}
