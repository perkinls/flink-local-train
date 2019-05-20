package com.lp.test.dataset

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ListBuffer

/**
  *
  */
object DataSetTransformationApp {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    mapFunction(env)
    filterFunction(env)
    mapPartitionFunction(env)
    firstFunction(env)
    flatMapFunction(env)
    distinctFunction(env)
    joinFunction(env)
    outerJoinFunction(env)
    crossFunction(env)
  }

  /**
    * 转换算子 笛卡尔积
    * @param env
    */
  def crossFunction(env: ExecutionEnvironment): Unit = {
    val info1 = List("曼联", "曼城")
    val info2 = List(3, 1, 0)
    import org.apache.flink.api.scala._

    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)

    data1.cross(data2).print()

  }

  /**
    * 转换算子 左外连接、右外连接、全连接
    * @param env
    */
  def outerJoinFunction(env: ExecutionEnvironment): Unit = {
    val info1 = ListBuffer[(Int, String)]() // 编号  名字
    info1.append((1, "PK哥"));
    info1.append((2, "J哥"));
    info1.append((3, "小队长"));
    info1.append((4, "猪头呼"));

    val info2 = ListBuffer[(Int, String)]() // 编号  城市
    info2.append((1, "北京"));
    info2.append((2, "上海"));
    info2.append((3, "成都"));
    info2.append((5, "杭州"));

    import org.apache.flink.api.scala._

    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)

    //    data1.leftOuterJoin(data2).where(0).equalTo(0).apply((first,second)=>{
    //
    //      if(second == null) {
    //        (first._1, first._2, "-")
    //      } else {
    //        (first._1, first._2, second._2)
    //      }
    //    }).print()

    //    data1.rightOuterJoin(data2).where(0).equalTo(0).apply((first,second)=>{
    //
    //      if(first == null) {
    //        (second._1, "-", second._2)
    //      } else {
    //        (first._1, first._2, second._2)
    //      }
    //    }).print()

    data1.fullOuterJoin(data2).where(0).equalTo(0).apply((first, second) => {

      if (first == null) {
        (second._1, "-", second._2)
      } else if (second == null) {
        (first._1, first._2, "-")
      } else {
        (first._1, first._2, second._2)
      }
    }).print()
  }

  /**
    * 转换算子 join 内连接
    * @param env
    */
  def joinFunction(env: ExecutionEnvironment): Unit = {
    val info1 = ListBuffer[(Int, String)]() // 编号  名字
    info1.append((1, "P哥"));
    info1.append((2, "J哥"));
    info1.append((3, "小队长"));
    info1.append((4, "猪头呼"));

    val info2 = ListBuffer[(Int, String)]() // 编号  城市
    info2.append((1, "北京"));
    info2.append((2, "上海"));
    info2.append((3, "成都"));
    info2.append((5, "杭州"));

    import org.apache.flink.api.scala._


    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)

    //where(0).equalTo(0) 分别代表两个数据集要进行join的字段
    //(first, second) 分别代表的两个数据集
    data1.join(data2).where(0).equalTo(0).apply((first, second) => {
      (first._1, first._2, second._2)
    }).print()

  }

  /**
    * 转换算子  去重
    *
    * @param env
    */
  def distinctFunction(env: ExecutionEnvironment): Unit = {
    val info = ListBuffer[String]()
    info.append("hadoop,spark")
    info.append("hadoop,flink")
    info.append("flink,flink")

    import org.apache.flink.api.scala._
    val data = env.fromCollection(info)

    data.flatMap(_.split(",")).distinct().print()
  }


  /**
    * 转换算子 flatMap   将一个拆分成多个
    *
    * @param env
    */
  def flatMapFunction(env: ExecutionEnvironment): Unit = {
    val info = ListBuffer[String]()
    info.append("hadoop,spark")
    info.append("hadoop,flink")
    info.append("flink,flink")
    import org.apache.flink.api.scala._

    val data = env.fromCollection(info)
    //    data.print()
    //data.map(_.split(",")).print()
    //    data.flatMap(_.split(",")).print()

    data.flatMap(_.split(",")).map((_, 1)).groupBy(0).sum(1).print()
  }

  /**
    * 转换算子 first获取前n个元素
    *
    * @param env
    */
  def firstFunction(env: ExecutionEnvironment): Unit = {
    val info = ListBuffer[(Int, String)]()
    info.append((1, "Hadoop"));
    info.append((1, "Spark"));
    info.append((1, "Flink"));
    info.append((2, "Java"));
    info.append((2, "Spring Boot"));
    info.append((3, "Linux"));
    info.append((4, "VUE"));

    import org.apache.flink.api.scala._
    val data = env.fromCollection(info)

    //data.first(3).print()
    //data.groupBy(0).first(2).print()

    //分组后是取组内的第几个 而不是整体的前几个,进行组内的排序
    data.groupBy(0).sortGroup(1, Order.DESCENDING).first(2).print()
  }

  // DataSource 100个元素，把结果存储到数据库中
  /**
    * mapPartition 转换算子
    *
    * @param env
    */
  def mapPartitionFunction(env: ExecutionEnvironment): Unit = {
    val students = new ListBuffer[String]
    for (i <- 1 to 100) {
      students.append("student: " + i)
    }
    import org.apache.flink.api.scala._
    //设置并行度
    val data = env.fromCollection(students).setParallelism(5)

    //每个分区获取一个connection
    data.mapPartition(x => {
      val connection = DBUtils.getConection()
      println(connection + "....")
      DBUtils.returnConnection(connection)
      x
    }).print()

  }

  /**
    * filter 转换算子
    *
    * @param env
    */
  def filterFunction(env: ExecutionEnvironment): Unit = {
    //    val data = env.fromCollection(List(1,2,3,4,5,6,7,8,9,10))
    //    data.map(_ + 1).filter(_ > 5).print()

    import org.apache.flink.api.scala._
    env.fromCollection(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
      .map(_ + 1)
      .filter(_ > 5)
      .print()
  }

  /**
    * map 转换算子
    *
    * @param env
    */
  def mapFunction(env: ExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._
    val data = env.fromCollection(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    // 对data中的每个元素都做一个+1的操作
    // data.map((x:Int) => x + 1).print()
    // data.map((x) => x + 1).print()
    // data.map(x => x + 1).print()
    data.map(_ + 1).print()
  }

}
