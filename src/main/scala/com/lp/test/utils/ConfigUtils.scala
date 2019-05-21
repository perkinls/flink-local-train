package com.lp.test.utils

import com.typesafe.config.{Config, ConfigFactory}
import net.ceedubs.ficus.Ficus._

/**
  * <p/> 
  * <li>Description: 读取kafka配置文件
  *                  可用于切换不同环境切换
  * </li>
  * <li>@author: lipan@cechealth.cn</li> 
  * <li>Date: 2019-05-21 14:30</li> 
  */
object ConfigUtils {

  def apply(): KafKaConfig = apply(ConfigFactory.load("conf/kafkaConfig"))

  def apply(applicationConfig: Config): KafKaConfig = {

    val config = applicationConfig.getConfig("dev-kafka")
    val stringTopic: String = config.as[String]("string.topic")
    val jsonTopic: String = config.as[String]("json.topic")
    val kvTopic: String = config.as[String]("kv.topic")
    val groupId: String = config.as[String]("group.id")
    val myTopicList: List[String] = config.as[List[String]]("my.topicList")
    val bootstrapServers: String = config.as[String]("bootstrap.servers")

    KafKaConfig(stringTopic, jsonTopic, kvTopic, groupId, myTopicList, bootstrapServers)
  }

  def main(args: Array[String]): Unit = {
    val kafKaConfig=ConfigUtils.apply()
    println(kafKaConfig.toString)
  }
}

case class KafKaConfig(stringTopic: String,
                       jsonTopic: String,
                       kvTopic: String,
                       groupId: String,
                       myTopicList: List[String],
                       bootstrapServers: String)
