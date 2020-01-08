package com.lp.scala.demo.table

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.types.Row

/**
  * <p/> 
  * <li>Description: </li>
  * <li>@author: panli0226@sina.com</li> 
  * <li>Date: 2019-04-16 12:50</li> 
  */
object TableSQLAPI {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = BatchTableEnvironment.create(env)

    val filePath = ""

    import org.apache.flink.api.scala._

    // 已经拿到DataSet
    val csv = env.readCsvFile[SalesLog](filePath,ignoreFirstLine=true)

    // DataSet ==> Table
    val salesTable = tableEnv.fromDataSet(csv)

    // Table ==> table
    tableEnv.registerTable("sales", salesTable)

    // sql
    val resultTable = tableEnv.sqlQuery("select customerId, sum(amountPaid) money from sales group by customerId")

    tableEnv.toDataSet[Row](resultTable).print()

  }

  case class SalesLog(transactionId:String,
                      customerId:String,
                      itemId:String,
                      amountPaid:Double)
}
