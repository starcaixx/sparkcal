package com.lb.sparktest.ods

import java.util.ResourceBundle
import scala.util.control.Breaks.{break, breakable}
import com.alibaba.fastjson.{JSON, JSONObject}
import com.lb.util.{MyKafkaConsumer, MyKafkaSink}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}

object MaxwellDBAPP {

  private val bundle: ResourceBundle = ResourceBundle.getBundle("jdbc")

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]")
      .setAppName(getClass.getSimpleName)
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.kafka.maxRatePerPartition", "200")
      .set("spark.executor.instances", "2")
      .set("spark.default.parallelism", "5")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.streaming.concurrentJobs", "4")

    val processingInterval: Int = bundle.getString("processingInterval").toInt
    val ssc = new StreamingContext(conf, Seconds(processingInterval))

    val topic: String = bundle.getString("topic_ods")
    val dbIndex = bundle.getString("dbIndex").toInt
    val kafkaDS: InputDStream[(String, String)] = MyKafkaConsumer.getKafkaStream(dbIndex,topic, ssc)
    var ranges: Array[OffsetRange] = Array.empty[OffsetRange]
    val recordDS: DStream[(String, String)] = kafkaDS.transform(rdd => {
      ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })
    val jsonDS: DStream[JSONObject] = recordDS.map(record => {
      val str: String = record._2
      JSON.parseObject(str)
    })

    jsonDS.print(10)
    jsonDS.foreachRDD(rdd => {
      rdd.foreachPartition(jsonItr => {
        if (ranges != null && ranges.size > 0) {
          val offsetRange = ranges(TaskContext.get().partitionId())
//          println("from:" + offsetRange.fromOffset + "----to:" + offsetRange.untilOffset)
        }
        for (elem <- jsonItr) {
          breakable{
            if (!"order_info".equals(elem.getString("table")) && !"order_detail".equals(elem.getString("table"))){
              break()
            }else{
              if (!"bootstrap-start".equals(elem.getString("type")) && !"bootstrap-complete".equals(elem.getString("type"))) {
                println("elem:"+elem)
                val tbName: String = elem.getString("table")
                val topic = "ODS_T_" + tbName.toUpperCase()
                val dataObj: JSONObject = elem.getJSONObject("data")
                if (!dataObj.isEmpty){
                  val key = tbName + "_" + dataObj.getString("id")
                  MyKafkaSink.send(topic,key,dataObj.toJSONString)
                }
              }
            }
          }


        }
      })
      MyKafkaConsumer.saveOffsetToRedis(dbIndex, ranges)
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
