package com.hxr.hotitems_analysis

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
 * @Description: 将文件写入到Kakfa中
 * @Author: CJ
 * @Data: 2020/12/26 10:25
 */
object KafkaProducerUtil {
  def main(args:Array[String]):Unit = {
    writeToKafka("hotitems")
  }

  def writeToKafka(topic: String): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","192.168.32.242:9092")
    properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](properties)

    // 从文件读取数据，逐行写入kafka
    val bufferedSource = io.Source.fromFile("C:\\Users\\Administrator\\Desktop\\不常用的项目\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

    for (line <- bufferedSource.getLines()) {
      val record = new ProducerRecord[String, String](topic, line)
      producer.send(record)
    }

    producer.close()
  }

}
