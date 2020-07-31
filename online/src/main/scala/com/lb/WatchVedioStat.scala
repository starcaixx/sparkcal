package com.lb

import java.net.URLDecoder
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}
import java.util.ResourceBundle

import com.alibaba.fastjson.{JSON, JSONObject}
import com.lb.util.{JdbcUtils, MyKafkaConsumer}
import net.ipip.ipdb.{City, CityInfo}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object StatWatchVedioCount {
  private val bundle: ResourceBundle = ResourceBundle.getBundle("jdbc")
  val db = new City(getClass.getClassLoader.getResource("ipipfree.ipdb").getPath)

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      print("Usage:please input checkpoint")
      System.exit(1)
    }
    val checkpoint = args(0)

    val streamingContext = StreamingContext.getOrCreate(checkpoint, () => getWatchVedioCount(checkpoint))
    streamingContext.start()
    streamingContext.awaitTermination()
    streamingContext.stop()
  }

  def getWatchVedioCount(checkpoint: String): StreamingContext = {
    val interval: Long = bundle.getString("processingInterval").toLong
    val topic: String = bundle.getString("topic")
    val dbIndex = bundle.getString("dbIndex").toInt

    val conf: SparkConf = new SparkConf().setAppName(getClass.getSimpleName)
      .set("spark.streaming.stopGracefullyOnShutdown", "true") //优雅关闭
      .set("spark.streaming.backpressure.enabled", "true") //开启背压
      .set("spark.streaming.kafka.maxRatePerPartition", "200") //每秒钟处理数据量
      .set("spark.executor.instances", "2")
      .set("spark.default.parallelism", "4")
      .set("spark.sql.shuffle.partitions", "4")

    val ssc = new StreamingContext(conf, Seconds(interval))

    ssc.sparkContext.setLogLevel("Error")
    /*存储在mysql中的offset
    val fromOffsets: Map[TopicAndPartition, Long] = DB.readOnly { implicit session =>
      sql"select topic,part_id,offset from topic_offset".map { r =>
      TopicAndPartition(r.string(1), r.int(2)) -> r.long(3)
      }.list.apply().toMap
    }*/

    val kafkaDStream = MyKafkaConsumer.getKafkaStream(topic, ssc)

    var offsetRanges = Array.empty[OffsetRange]
    val watchuiDStream: DStream[WatchUserInfo] = kafkaDStream.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      val jedis: Jedis = JdbcUtils.getJedisClient
      val localDate: LocalDate = LocalDate.now()
      jedis.expire(localDate.toString.replace("-", "") + "_watchvedio", 60 * 60 * 30)
      jedis.close()
      rdd
    })
      .map(msg => {
        getCountAndDate(msg)
      })

    //record-去重,统计当日看课指标
    watchuiDStream.foreachRDD(rdd => {
      val tuples: Array[(String, String)] = rdd.map(userinfo => (userinfo.date, userinfo.uid)).distinct().collect()
      val jedis: Jedis = JdbcUtils.getJedisClient
      for (elem <- tuples) {
        jedis.sadd(elem._1 + "_watchvedio", elem._2)
      }
      jedis.close()
      MyKafkaConsumer.saveOffsetToRedis(dbIndex, offsetRanges)
    })

    //开启检查点
    ssc.checkpoint(checkpoint)
    //批处理时间的5-10倍
    kafkaDStream.checkpoint(Seconds(interval * 10))
    //保存到文件
    //	resultDStream.saveAsTextFiles("")
    ssc
  }

  //  val updateFunc: (Seq[Int], Option[Int]) => Option[Int] = {}

  def getCountAndDate(msg: (String, String)): WatchUserInfo = {
    val decode = URLDecoder.decode(msg._2.split("\t")(9), "utf-8")
    val jsonObject: JSONObject = JSON.parseObject(decode.substring(decode.indexOf("{")))
    //	datas
    val localDateTime: LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(jsonObject.getLongValue("ts")), ZoneId.systemDefault())
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")

    val dateStr: String = localDateTime.format(formatter)
    val cityInfo: CityInfo = db.findInfo(jsonObject.getString("ip"), "CN")
    var regionName = "未知"
    regionName = cityInfo.getRegionName
    WatchUserInfo(jsonObject.getString("uid"),
      regionName,
      jsonObject.getJSONObject("value").getString("cid"),
      jsonObject.getJSONObject("value").getString("cpid"),
      jsonObject.getJSONObject("value").getString("cosid"),
      jsonObject.getJSONObject("value").getLong("wsize"),
      jsonObject.getLong("ts"),
      dateStr)
  }

}

case class WatchUserInfo(uid: String, regin: String, cid: String, cpid: String, cosid: String, wsize: Long, ts: Long, date: String)