package com.lb.sparktest.dim

import java.util.ResourceBundle

import com.alibaba.fastjson.{JSON, JSONObject}
import com.lb.sparktest.bean.ProvinceInfo
import com.lb.util.MyKafkaConsumer
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DimensionTableSync {

  private val bundle: ResourceBundle = ResourceBundle.getBundle("jdbc")
  def main(args: Array[String]): Unit = {

    val checkpoint = "/Users/longbei/IdeaProjects/sparkcal/sparktest/dim"
    val sc: StreamingContext = StreamingContext.getOrCreate(checkpoint,()=>syncDimTab(checkpoint))

    sc.start()
    sc.awaitTermination()
  }

  def syncDimTab(checkpoint: String): StreamingContext = {
    val interval = bundle.getString("processingInterval").toInt
    val topic: String = bundle.getString("topic_dim")
    val dbIndex = bundle.getString("dbIndex").toInt
    val conf: SparkConf = new SparkConf().setAppName(getClass.getSimpleName)
      .setMaster("local[4]")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.kafka.maxRatePerPartition", "200")
      .set("spark.executor.instances", "2")
      .set("spark.default.parallelism", "5")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.streaming.concurrentJobs", "4")

    val ssc = new StreamingContext(conf,Seconds(interval))

    val kafkaDS: InputDStream[(String, String)] = MyKafkaConsumer.getKafkaStream(topic,ssc)
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val transformDS: DStream[(String, String)] = kafkaDS.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })
    val provinceDS: DStream[ProvinceInfo] = transformDS.map(record => {
      JSON.parseObject(record._2, classOf[ProvinceInfo])
    })

    provinceDS.foreachRDD(rdd=>{
      import org.apache.phoenix.spark._
      rdd.saveToPhoenix("GMALL_PROVINCE_INFO",Seq("ID","NAME","REGION_ID","AREA_CODE"),new Configuration,Some("master:2181"))
      MyKafkaConsumer.saveOffsetToRedis(dbIndex,offsetRanges)
//      save offset to redis
//        fromOffset:0:untilOffset:2
    })

    ssc
  }

}
