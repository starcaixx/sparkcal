package com.lb.sparktest.dim

import java.util.ResourceBundle

import com.alibaba.fastjson.JSON
import com.lb.sparktest.bean.BaseCategory3
import com.lb.util.MyKafkaConsumer
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object BaseCategory3App {

  private val bundle: ResourceBundle = ResourceBundle.getBundle("jdbc")
  def main(args: Array[String]): Unit = {
    val checkpoint = "/Users/longbei/IdeaProjects/sparkcal/sparktest/basecategory"
    val ssc: StreamingContext = StreamingContext.getOrCreate(checkpoint,()=>baseCategorySync(checkpoint))

    ssc.start()
    ssc.awaitTermination()
  }

  def baseCategorySync(checkpoint: String): StreamingContext = {
    val dbIndex: Int = bundle.getString("localDbIndex").toInt
    val topic = "ODS_T_BASE_CATEGORY3"
    val conf: SparkConf = new SparkConf().setAppName(getClass.getSimpleName)
      .setMaster("local[4]")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.kafka.maxRatePerPartition", "200")
      .set("spark.executor.instances", "2")
      .set("spark.default.parallelism", "5")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.streaming.concurrentJobs", "4")
    val ssc = new StreamingContext(conf,Seconds(10))

    val kafkaDS: InputDStream[(String, String)] = MyKafkaConsumer.getKafkaStream(dbIndex,topic,ssc)
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val transFormDS: DStream[(String, String)] = kafkaDS.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })
    transFormDS.map(record=>{
      JSON.parseObject(record._2,classOf[BaseCategory3])
    }).foreachRDD(rdd=>{
      import org.apache.phoenix.spark._
      rdd.saveToPhoenix("GMALL_BASE_CATEGORY3",Seq("ID","NAME","CATEGORY2_ID"),new Configuration(),Some("node:2181"))
      MyKafkaConsumer.saveOffsetToRedis(dbIndex,offsetRanges)
    })

    ssc
  }

}
