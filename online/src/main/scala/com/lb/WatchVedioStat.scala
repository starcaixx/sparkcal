package com.lb

import java.net.URLDecoder
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}
import java.util
import java.util.ResourceBundle

import com.alibaba.fastjson.{JSON, JSONObject}
import com.lb.util.JdbcUtils
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import net.ipip.ipdb.{City, CityInfo}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
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

	val streamingContext = StreamingContext.getOrCreate(checkpoint,()=>getWatchVedioCount(checkpoint))
	streamingContext.start()
	streamingContext.awaitTermination()
	streamingContext.stop()
  }

  def getWatchVedioCount(checkpoint: String): StreamingContext = {
	val interval: Long = bundle.getString("processingInterval").toLong
	val brokers: String = bundle.getString("brokers")
	val topic: String = bundle.getString("topic")
	val dbIndex = bundle.getString("dbIndex").toInt

	val conf: SparkConf = new SparkConf().setAppName(getClass.getSimpleName)
  	.set("spark.streaming.stopGracefullyOnShutdown","true")//优雅关闭
  	.set("spark.streaming.backpressure.enabled","true")//开启背压
  	.set("spark.streaming.kafka.maxRatePerPartition","200")//每秒钟处理数据量
  	.set("spark.executor.instances","2")
  	.set("spark.default.parallelism","4")
  	.set("spark.sql.shuffle.partitions","4")

	val ssc = new StreamingContext(conf,Seconds(interval))

	ssc.sparkContext.setLogLevel("Error")
	val kafkaParams: Map[String, String] = Map[String,String](
//	  "metadata.broker.list" -> brokers,
	  "bootstrap.servers" -> brokers,
	  "group.id" -> "mygroup",
	  /**
		* 当没有初始的offset，或者当前的offset不存在，如何处理数据
		*  earliest ：自动重置偏移量为最小偏移量
		*  latest：自动重置偏移量为最大偏移量【默认】
		*  none:没有找到以前的offset,抛出异常
		*/
	  "auto.offset.reset" -> "latest",
	  "enable.auto.commit" -> "false"
	)
	/*存储在mysql中的offset
	val fromOffsets: Map[TopicAndPartition, Long] = DB.readOnly { implicit session =>
	  sql"select topic,part_id,offset from topic_offset".map { r =>
		TopicAndPartition(r.string(1), r.int(2)) -> r.long(3)
	  }.list.apply().toMap
	}*/

	val fromOffsets = getOffsetFromRedis(dbIndex,topic).map(r=>{
	  TopicAndPartition(topic,r._1.toInt)->r._2.toLong
	}).toMap

	println("fromOffsets:"+fromOffsets)
	val messageHandler = (mmd:MessageAndMetadata[String, String]) => (mmd.topic,mmd.message())

	val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder,(String,String)](ssc,kafkaParams,fromOffsets,messageHandler)

	var offsetRanges = Array.empty[OffsetRange]
	val watchuiDStream: DStream[WatchUserInfo] = kafkaDStream.transform(rdd => {
	  offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
	  val jedis: Jedis = JdbcUtils.jedisPool.getResource
	  val localDate: LocalDate = LocalDate.now()
	  jedis.expire(localDate.toString.replace("-","")+"_watchvedio",60*60*30)
	  jedis.close()
	  rdd
	})
	  .map(msg => {
		getCountAndDate(msg)
	  })

	//record-去重,统计当日看课指标
	watchuiDStream.foreachRDD(rdd=>{
	  val tuples: Array[(String, String)] = rdd.map(userinfo=>(userinfo.date,userinfo.uid)).distinct().collect()
	  val jedis: Jedis = JdbcUtils.jedisPool.getResource
	  for (elem <- tuples) {
		jedis.sadd(elem._1+"_watchvedio",elem._2)
	  }
	  jedis.close()
	  saveOffsetToRedis(dbIndex,offsetRanges)
	})

	//开启检查点
	ssc.checkpoint(checkpoint)
	//批处理时间的5-10倍
	kafkaDStream.checkpoint(Seconds(interval*10))
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
	val cityInfo: CityInfo = db.findInfo(jsonObject.getString("ip"),"CN")
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

  /**
	* 保存offset到redis
	* @param db
	* @param offsetRanges
	*/
  def saveOffsetToRedis(db:Int,offsetRanges:Array[OffsetRange])={
	val jedis: Jedis = JdbcUtils.jedisPool.getResource
	jedis.select(db)
	for (elem <- offsetRanges) {
	  jedis.hset(elem.topic,elem.partition.toString,elem.untilOffset.toString)
	}
	jedis.close()
  }

  /**
	* 从redis中获取保存的消费者offset
	* @param db
	* @param topic
	* @return
	*/
  def getOffsetFromRedis(db:Int,topic:String)={
	val jedis: Jedis = JdbcUtils.jedisPool.getResource
	jedis.select(db)
	val result: java.util.Map[String, String] = jedis.hgetAll(topic)
	jedis.close()
	if (result.size()==0) {
	  result.put("0","0")
	  result.put("1","0")
	  result.put("2","0")
	}
	import scala.collection.JavaConversions.mapAsScalaMap
	val offsetMap: scala.collection.mutable.Map[String,String] = result
	offsetMap
  }
}

case class WatchUserInfo(uid:String,regin:String,cid:String,cpid:String,cosid:String,wsize:Long,ts:Long,date:String)