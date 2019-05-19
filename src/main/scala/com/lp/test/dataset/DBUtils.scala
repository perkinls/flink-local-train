package com.lp.test.dataset

import scala.util.Random

/**
  * 模拟数据库连接
  */
object DBUtils {

  def getConection(): String = {
    new Random().nextInt(10) + ""
  }

  def returnConnection(connection:String): Unit ={

  }

}
