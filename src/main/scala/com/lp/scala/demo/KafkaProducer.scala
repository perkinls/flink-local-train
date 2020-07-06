package com.lp.scala.demo

import java.util.Properties
import net.sf.json.JSONObject
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.util.Random

/**
  *
  * <li>Description: kafka生产者</li>
  * <li>@author: panli0226@sina.com</li> 
  * <li>Date: 2019-05-07 21:54</li> 
  */
object KafkaProducer {
  def main(args: Array[String]): Unit = {

    val props = new Properties()
    props.setProperty("bootstrap.servers", "localhost:9092")
    props.setProperty("acks", "all")
    props.setProperty("retries", "0")
    props.setProperty("batch.size", "16384")
    props.setProperty("linger.ms", "1")
    props.setProperty("buffer.memory", "33554432")
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)

    val producer = {
      new KafkaProducer[String, String](props)
    }
    var random = new Random(2)

    //指定发送任意格式的数据到kafka
    while (true) {
            producer.send(new ProducerRecord[String, String]("fk_string_topic", String.valueOf(random.nextInt(100))))
      //      sendMsgJson(producer)
      //            sendMsgKv(producer)
      //      sendMsgEvent(producer)
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

      println("发送到kafak数据格式" + String.valueOf(i) + "：" + json.toString)

      try {
        Thread.sleep(1000)
      } catch {
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
    val points: Random = new Random()
    val arrayBuffer = Array[String]("apple", "pear", "nut", "grape", "banana", "pineapple", "pomelo", "orange")

    for (i <- 0 until 10) {

      //join情况
      val str = arrayBuffer(points.nextInt(8)) + " " + points.nextInt(5) + " " + System.currentTimeMillis
      producer.send(new ProducerRecord[String, String]("fk_kv_topic", String.valueOf(i), str))
//      producer.send(new ProducerRecord[String, String]("fk_kv_topic", String.valueOf(i) + "01", str))
      producer.send(new ProducerRecord[String, String]("fk_kv_1_topic", String.valueOf(i), str))
//      producer.send(new ProducerRecord[String, String]("fk_kv_1_topic", String.valueOf(i) + "02", str))

      println("first  Kv:" + String.valueOf(i) + "-1" + ":======>" + str)
      println("Second Kv:" + String.valueOf(i) + "-2" + ":======>" + str)

      //非join情况
//      var str = arrayBuffer(points.nextInt(8)) + " " + points.nextInt(5)
      //      producer.send(new ProducerRecord[String, String]("fk_kv_topic", String.valueOf(i), str))
      //      println("first Kv:" + String.valueOf(i) + ":======>" + str)
      //      str = arrayBuffer(points.nextInt(8)) + " " + points.nextInt(5)
      //      producer.send(new ProducerRecord[String, String]("fk_kv_1_topic", String.valueOf(i), str))
      //      println("Second Kv_1:" + String.valueOf(i) + ":======>" + str)

      try {
        Thread.sleep(1000)
      } catch {
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
      try {
        Thread.sleep(1000)
      } catch {
        case e: InterruptedException => e.printStackTrace()
      }

    }

  }
}
