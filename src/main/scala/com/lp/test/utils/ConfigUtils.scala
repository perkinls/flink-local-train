package com.lp.test.utils

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import net.ceedubs.ficus.Ficus._


/**
  * <p/> 
  * <li>Description: 读取kafka配置文件
  * 可用于切换不同环境切换
  * </li>
  * <li>@author: lipan@cechealth.cn</li> 
  * <li>Date: 2019-05-21 14:30</li> 
  */
object ConfigUtils {


  def apply(topicName: String) = {
    val applicationConfig = ConfigFactory.load("conf/kafkaConfig")
    val config = applicationConfig.getConfig("dev-kafka")
    val stringTopic: String = config.as[String]("string.topic")
    val jsonTopic: String = config.as[String]("json.topic")
    val kvTopic: String = config.as[String]("kv.topic")
    val kv1Topic: String = config.as[String]("kv1.topic")
    val groupId: String = config.as[String]("group.id")
    val bootstrapServers: String = config.as[String]("bootstrap.servers")

    val props = new Properties()
    props.setProperty("bootstrap.servers", bootstrapServers)
    props.setProperty("group.id", groupId)

    topicName match {
      case "string" =>
        (stringTopic, props)
      case "kv" =>
        (kvTopic, props)
      case "kv1" =>
        (kv1Topic, props)
      case "json" =>
        (jsonTopic, props)
    }

  }
}


