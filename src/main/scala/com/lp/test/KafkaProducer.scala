package com.lp.test

import java.lang.Thread.sleep
import java.util.Properties

import net.sf.json.JSONObject
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.util.Random

/**
  * <p/> 
  * zk: mg01.cechealth.cn:2181,mg02.cechealth.cn:2181,mg03.cechealth.cn:2181
  * brokers:dn01.cechealth.cn:9092,dn02.cechealth.cn:9092,dn03.cechealth.cn:9092
  * <li>Description: kafka生产者</li>
  * <li>@author: lipan@cechealth.cn</li> 
  * <li>Date: 2019-05-07 21:54</li> 
  */
object KafkaProducer {
  def main(args: Array[String]): Unit = {

    val props = new Properties()
    //    props.setProperty("bootstrap.servers", "master:9092")
    props.setProperty("bootstrap.servers", "dn01.cechealth.cn:6667,dn02.cechealth.cn:6667,dn03.cechealth.cn:6667")
    props.setProperty("acks", "all")
    props.setProperty("retries", "0")
    props.setProperty("batch.size", "16384")
    props.setProperty("linger.ms", "1")
    props.setProperty("buffer.memory", "33554432")
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)

    val producer = new KafkaProducer[String, String](props)
    var random = new Random(2)

    //指定发送任意格式的数据到kafka
    for (e <- 0 until 100000) {


      producer.send(new ProducerRecord[String, String]("fk_string_topic", String.valueOf(random.nextInt(10))))
      //      sendMsgJson(producer)
//      sendMsgKv(producer)
      //      sendMsgEvent(producer)

      try
        sleep(1000)
      catch {
        case e: InterruptedException => e.printStackTrace()
      }
    }

  }

  /**
    * 向kafka中发送Json格式数据到kafka
    *
    * @param producer
    */
  def sendMsgJson(producer: Producer[String, String]) = {
    val points: Random = new Random(2)
    val arrayBuffer = Array[String]("apple", "pear", "nut", "grape", "banana", "pineapple", "pomelo", "orange")

    for (i <- 0 until 100) {
      val json: JSONObject = new JSONObject
      json.put("fruit", arrayBuffer(points.nextInt(8)))
      json.put("number", points.nextInt(4))
      json.put("time", System.currentTimeMillis)
      producer.send(new ProducerRecord[String, String]("fk_json_topic", String.valueOf(i), json.toString))

      try
        sleep(1000)
      catch {
        case e: InterruptedException => e.printStackTrace()
      }
    }

  }

  /**
    * 发送kv格式的数据到kafka
    *
    * @param producer
    */
  def sendMsgKv(producer: Producer[String, String]): Unit = {
    val points: Random = new Random(2)
    val arrayBuffer = Array[String]("apple", "pear", "nut", "grape", "banana", "pineapple", "pomelo", "orange")

    for (i <- 0 until 10) {

      //join情况
      var str = arrayBuffer(points.nextInt(8)) + " " + points.nextInt(5) + " " + System.currentTimeMillis
      producer.send(new ProducerRecord[String, String]("fk_kv_topic", String.valueOf(i), str))
      producer.send(new ProducerRecord[String, String]("fk_kv_topic", String.valueOf(i) + "01", str))
      producer.send(new ProducerRecord[String, String]("fk_kv_1_topic", String.valueOf(i), str))
      producer.send(new ProducerRecord[String, String]("fk_kv_1_topic", String.valueOf(i) + "02", str))

      println("first  Kv:" + String.valueOf(i) + "-1" + ":======>" + str + "\t" + str)
      println("Second Kv:" + String.valueOf(i) + "-2" + ":======>" + str + "\t" + str)

      //非join情况
      //      var str = arrayBuffer(points.nextInt(8)) + " " + points.nextInt(5)
      //      producer.send(new ProducerRecord[String, String]("fk_kv_topic", String.valueOf(i), str))
      //      println("first Kv:" + String.valueOf(i) + ":======>" + str)
      //      str = arrayBuffer(points.nextInt(8)) + " " + points.nextInt(5)
      //      producer.send(new ProducerRecord[String, String]("fk_kv_1_topic", String.valueOf(i), str))
      //      println("Second Kv_1:" + String.valueOf(i) + ":======>" + str)

      try
        sleep(1000)
      catch {
        case e: InterruptedException => e.printStackTrace()
      }

    }

  }

  /**
    * 发送event格式数据到kafka
    *
    * @param producer
    */
  def sendMsgEvent(producer: Producer[String, String]): Unit = {
    val points: Random = new Random(2)
    val arrayBuffer = Array[String]("apple", "pear", "nut", "grape", "banana", "pineapple", "pomelo", "orange")
    for (i <- 0 until 3000) {
      val str = arrayBuffer(points.nextInt(8)) + "," + points.nextInt(5) + "," + System.currentTimeMillis
      producer.send(new ProducerRecord[String, String]("fk_event_topic", String.valueOf(i), str))
      try
        sleep(1000)
      catch {
        case e: InterruptedException => e.printStackTrace()
      }

    }

  }
}
