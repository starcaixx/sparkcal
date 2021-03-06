package com.lb.sparktest.dim

import java.util.ResourceBundle

import com.alibaba.fastjson.JSON
import com.lb.sparktest.bean.BaseTrademark
import com.lb.util.MyKafkaConsumer
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object BaseTrademarkApp {
  private val bundle: ResourceBundle = ResourceBundle.getBundle("jdbc")
  def main(args: Array[String]): Unit = {
    val checkpoint = "/Users/longbei/IdeaProjects/sparkcal/sparktest/basetrade"
    val ssc: StreamingContext = StreamingContext.getOrCreate(checkpoint,()=>baseTradeSync(checkpoint))
    ssc.start()
    ssc.awaitTermination()
  }

  def baseTradeSync(checkpoint: String): StreamingContext = {
    val dbIndex: Int = bundle.getString("localDbIndex").toInt
    val topic = "ODS_T_BASE_TRADEMARK"
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
      offsetRanges=rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })
    transFormDS.map(record=>{
      JSON.parseObject(record._2,classOf[BaseTrademark])
    }).foreachRDD(rdd=>{
      import org.apache.phoenix.spark._
      rdd.saveToPhoenix("GMALL_BASE_TRADEMARK",Seq("ID","TM_NAME"),new Configuration(),Some("node:2181"))
      MyKafkaConsumer.saveOffsetToRedis(dbIndex,offsetRanges)
    })

    ssc
  }
}
