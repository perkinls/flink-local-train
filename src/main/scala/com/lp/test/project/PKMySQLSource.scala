package com.lp.test.project

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.collection.mutable

/**
  * <p/> 
  * <li>Description:
  *
  * <li>@author: lipan@cechealth.cn</li> 
  * <li>Date: 2019-04-28 21:15</li> 
  */
class PKMySQLSource extends RichParallelSourceFunction[mutable.HashMap[String,String]]{
  override def cancel(): Unit = ???

  override def run(sourceContext: SourceFunction.SourceContext[mutable.HashMap[String, String]]): Unit = ???
}
