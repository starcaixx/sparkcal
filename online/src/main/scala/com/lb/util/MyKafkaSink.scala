package com.lb.util

import java.util.{Properties, ResourceBundle}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object MyKafkaSink {

  private val bundle: ResourceBundle = ResourceBundle.getBundle("jdbc")
  private val brokers: String = bundle.getString("brokers")

  var kafkaProducer :KafkaProducer[String,String] = null
  def createKafkaProducer:KafkaProducer[String,String]={

    val properties = new Properties()
    properties.put("bootstrap.servers",brokers)
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("enable.idompotence",(true: java.lang.Boolean))

    val producer = new KafkaProducer[String,String](properties)
    producer
  }

  def send(topic:String,msg:String): Unit ={
    if (kafkaProducer==null) {
      kafkaProducer=createKafkaProducer
    }
    kafkaProducer.send(new ProducerRecord[String,String](topic,msg))
  }

  def send(topic:String,key:String,msg:String): Unit ={
    if (kafkaProducer==null) {
      kafkaProducer=createKafkaProducer
    }
    kafkaProducer.send(new ProducerRecord[String,String](topic,key,msg))
  }
}
