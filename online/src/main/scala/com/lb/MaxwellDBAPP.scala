package com.lb

import java.util.ResourceBundle

import com.alibaba.fastjson.{JSON, JSONObject}
import com.lb.util.MyKafkaConsumer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}

object MaxwellDBAPP {

  private val bundle: ResourceBundle = ResourceBundle.getBundle("maxwell")

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
    val kafkaDS: InputDStream[(String, String)] = MyKafkaConsumer.getKafkaStream(topic, ssc)
    var ranges: Array[OffsetRange] = Array.empty[OffsetRange]
    val recordDS: DStream[(String, String)] = kafkaDS.transform(rdd => {
      ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })
    val jsonDS: DStream[JSONObject] = recordDS.map(record => {
      val str: String = record._2
      JSON.parseObject(str)
    })

    jsonDS.foreachRDD(rdd => {
      rdd.foreachPartition(jsonItr => {
        if (ranges != null && ranges.size > 0) {
          val offsetRange = ranges(TaskContext.get().partitionId())
          println("from:" + offsetRange.fromOffset + "----to:" + offsetRange.untilOffset)
        }
        for (elem <- jsonItr) {
          if ("".equals(elem.getString("type"))) {
            val tbName: String = elem.getString("table")
            val topic = "ODS_T_" + tbName.toUpperCase()
            val key = tbName + "_" + elem.getJSONObject("data").getString("id")
            //            MyKafkaSink.send(topic,key,elem.toJSONString)
          }
        }
      })
      MyKafkaConsumer.saveOffsetToRedis(dbIndex, ranges)
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
