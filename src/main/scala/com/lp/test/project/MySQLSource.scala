package com.lp.test.project

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.collection.mutable

/**
  * <p/> 
  * <li>Description:
  * 自定义Mysql 并行的Source
  * <li>@author: lipan@cechealth.cn</li> 
  * <li>Date: 2019-04-28 21:15</li> 
  */
class MySQLSource extends RichParallelSourceFunction[mutable.HashMap[String, String]] {

  var connection: Connection = _
  var pstmt: PreparedStatement = _

  override def cancel(): Unit = {}

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
    val sql = "select user_id,domain from user_domain_config"
    pstmt = connection.prepareStatement(sql)
    System.out.println("open")
  }

  /**
    * 获取数据库连接
    */
  def getConnection() = {
    DriverManager.getConnection("jdbc:mysql://localhost:3306/test.user_domain_config?user=root&password=1234")
  }

  /**
    * 从mysql表中将数据读取出来转成Map进行数据的封装
    * @param sourceContext
    */
  override def run(sourceContext: SourceFunction.SourceContext[mutable.HashMap[String, String]]): Unit = {

    //Todo ....
  }

  /**
    * 在close方法中要释放资源
    *
    * @throws Exception
    */
  @throws[Exception]
  override def close(): Unit = {
    super.close()
    if (pstmt != null) pstmt.close()
    if (connection != null) connection.close()
  }
}
