package com.lb.sparktest.ods

import java.util.ResourceBundle

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.lb.util.{MyKafkaConsumer, MyKafkaSink}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}


object CanalDBAPP {
  private val bundle: ResourceBundle = ResourceBundle.getBundle("jdbc")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[4]")
      .setAppName(getClass.getSimpleName)
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.kafka.maxRatePerPartition", "200")
      .set("spark.executor.instances", "2")
      .set("spark.default.parallelism", "5")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.streaming.concurrentJobs", "4") //这个参数?

    val ssc = new StreamingContext(conf, Seconds(10))

    val topic: String = bundle.getString("topic")
    val dbIndex = bundle.getString("dbIndex").toInt
    val kafkaDS: InputDStream[(String, String)] = MyKafkaConsumer.getKafkaStream(dbIndex,topic, ssc)
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val recordDS: DStream[(String, String)] = kafkaDS.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })

    val jsonDS: DStream[JSONObject] = recordDS.map(record => {
      val jsonString: String = record._2
      JSON.parseObject(jsonString)
    })
    jsonDS.foreachRDD(rdd => {
      rdd.foreachPartition(jsonItr => {
        if (offsetRanges != null && offsetRanges.size > 0) {
          val offsetRange = offsetRanges(TaskContext.get().partitionId())
          println("from:" + offsetRange.fromOffset + "------to:" + offsetRange.untilOffset)
        }
        for (elem <- jsonItr) {
          val tbName: String = elem.getString("table")
          val dataJsonArray: JSONArray = elem.getJSONArray("data")

          for (i <- 0 until dataJsonArray.size()) {
            val jsonObj: JSONObject = dataJsonArray.getJSONObject(i)
            val topic: String = "DW_" + tbName.toUpperCase
            val key: String = tbName + "_" + jsonObj.getString("id")
            //发送到各自topic
            MyKafkaSink.send(topic,key,jsonObj.toJSONString)
          }
        }
      })
      MyKafkaConsumer.saveOffsetToRedis(dbIndex, offsetRanges)
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
