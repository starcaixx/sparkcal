package com.lb.sparktest

import java.lang
import java.time.LocalDate
import java.util.ResourceBundle

import com.alibaba.fastjson.{JSON, JSONObject}
import com.lb.sparktest.bean.DauInfo
import com.lb.util.{JdbcUtils, MyEsUtil, MyKafkaConsumer}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object RealtimeDau {

  private val bundle: ResourceBundle = ResourceBundle.getBundle("jdbc")

  //  {"commonon":{"ar":"230000","ba":"Huawei","ch":"360","md":"Huawei P30","mid":"mid_32","os":"Android 11.0","uid":"300","vc":"v2.1.134"},"displays":[{"display_type":"activity","item":"2","item_type":"activity_id","order":1},{"display_type":"promotion","item":"6","item_type":"sku_id","order":2},{"display_type":"query","item":"5","item_type":"sku_id","order":3},{"display_type":"promotion","item":"2","item_type":"sku_id","order":4},{"display_type":"query","item":"8","item_type":"sku_id","order":5},{"display_type":"promotion","item":"6","item_type":"sku_id","order":6},{"display_type":"query","item":"4","item_type":"sku_id","order":7},{"display_type":"query","item":"8","item_type":"sku_id","order":8},{"display_type":"promotion","item":"4","item_type":"sku_id","order":9},{"display_type":"query","item":"7","item_type":"sku_id","order":10},{"display_type":"query","item":"6","item_type":"sku_id","order":11}],"page":{"during_time":4545,"page_id":"home"},"ts":1595581701544}
  def main(args: Array[String]): Unit = {
    val checkpoint = "/Users/longbei/IdeaProjects/sparkcal/sparktest/ck"
    val ssc: StreamingContext = StreamingContext.getOrCreate(checkpoint, () => dauStat(checkpoint))

    ssc.start()
    ssc.awaitTermination()
  }

  def dauStat(checkpoint: String): StreamingContext = {
    val topic = bundle.getString("malltopic")
    val interval = bundle.getString("processingInterval").toInt
    val dbIndex = bundle.getString("dbIndex").toInt
    val partitions = bundle.getString("partitioncnt").toInt
    //    val rediskeyexists: Int = bundle.getString("rediskeyexists").toInt
    val conf: SparkConf = new SparkConf().setAppName(getClass.getSimpleName).setMaster("local[4]")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.kafka.maxRatePerPartition", "200")
      .set("spark.executor.instances", "2")
      .set("spark.default.parallelism", "5")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.streaming.concurrentJobs", "4")
    //这个参数?
    val ssc = new StreamingContext(conf, Seconds(interval))
    val kafkaDS: InputDStream[(String, String)] = MyKafkaConsumer.getKafkaStream(dbIndex,topic, ssc)
    kafkaDS.print(10)
    var ranges: Array[OffsetRange] = Array[OffsetRange]()
    val transformDS: DStream[(String, String)] = kafkaDS.transform(rdd => {
      ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })
    val jsonDS: DStream[JSONObject] = transformDS.map(record => {
      JSON.parseObject(record._2)
    })

    val tupleDS: DStream[(String, lang.Long)] = jsonDS.map(json => {
      val ts: lang.Long = json.getLong("ts")
      import java.time.Instant
      import java.time.LocalDateTime
      import java.time.ZoneId
      val localDateTime: LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneId.systemDefault)
      (json.getJSONObject("common").getString("mid") + "~" + localDateTime.toLocalDate, ts)
    })
    val reduceDS: DStream[(String, lang.Long)] = tupleDS.reduceByKey((x, y) => x)

    val dauLoginDS: DStream[(String, lang.Long)] = reduceDS.mapPartitions(tupItr => {
      val jedis: Jedis = JdbcUtils.getJedisClient
      val list: List[(String, lang.Long)] = tupItr.toList
      val dauLoginList = new ListBuffer[Tuple2[String, lang.Long]]
      for (elem <- list) {
        val infos: Array[String] = elem._1.split("~")
        val isAdd: lang.Long = jedis.sadd("dau:" + infos(1), infos(0))
        if (1 == isAdd) {
          jedis.expire("dau:"+infos(1),3600*24*3)
          dauLoginList += elem
        }
      }
      jedis.close()

      dauLoginList.toIterator
    })

    val dauDS: DStream[DauInfo] = dauLoginDS.map(info => {
      val fields: Array[String] = info._1.split("~")
      DauInfo(fields(0), fields(1), info._2)
    })
    dauDS.foreachRDD(rdd => {
      rdd.foreachPartition(dauInfoItr => {
        val date: LocalDate = LocalDate.now()
        MyEsUtil.executeIndexBulk("gmall_dau_info_" + date, dauInfoItr.toList)
      })
      MyKafkaConsumer.saveOffsetToRedis(dbIndex, ranges)
    })

    //    ssc.checkpoint(checkpoint)
    //    kafkaDS.checkpoint(Seconds(interval*10))

    ssc
  }

}



