package com.lb.util

import java.util.ResourceBundle

import com.alibaba.fastjson.JSONObject
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.{KafkaUtils, OffsetRange}
import redis.clients.jedis.Jedis

object MyKafkaConsumer {
  private val bundle: ResourceBundle = ResourceBundle.getBundle("jdbc")
  val brokers: String = bundle.getString("brokers")
  private val dbIndex: Int = bundle.getString("dbIndex").toInt

  def getKafkaStream(topic:String, ssc:StreamingContext): InputDStream[(String,String)] ={

    val kafkaParams: Map[String, String] = Map[String,String](
      //	  "metadata.broker.list" -> brokers,
      "bootstrap.servers" -> brokers,
      "group.id" -> "mygroup",
      /**
        * 当没有初始的offset，或者当前的offset不存在，如何处理数据
        *  earliest ：自动重置偏移量为最小偏移量 smallest
        *  latest：自动重置偏移量为最大偏移量【默认】 largest
        *  none:没有找到以前的offset,抛出异常
        */
      "auto.offset.reset" -> "smallest",
      "enable.auto.commit" -> "false"
    )

    val fromOffsets = getOffsetFromRedis(dbIndex,topic).map(r=>{
      TopicAndPartition(topic,r._1.toInt)->r._2.toLong
    }).toMap

    val messageHandler = (mmd:MessageAndMetadata[String, String]) => (mmd.topic,mmd.message())

    val kafkaDS: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder,(String,String)](ssc,kafkaParams,fromOffsets,messageHandler)
    kafkaDS
  }

  /**
    * 从redis中获取保存的消费者offset
    * @param db
    * @param topic
    * @return
    */
  def getOffsetFromRedis(db:Int,topic:String)={
    val jedis: Jedis = JdbcUtils.getJedisClient
    jedis.select(db)
    val result: java.util.Map[String, String] = jedis.hgetAll(topic)
    jedis.close()
    println(result.size())
    if (result.size()==0) {
      result.put("0","0")
      result.put("1","0")
      result.put("2","0")
    }
    import scala.collection.JavaConversions.mapAsScalaMap
    val offsetMap: scala.collection.mutable.Map[String,String] = result
    offsetMap
  }

  /**
    * 保存offset到redis
    * @param db
    * @param offsetRanges
    */
  def saveOffsetToRedis(db:Int,offsetRanges:Array[OffsetRange])={
    println("save offset to redis")
    val jedis: Jedis = JdbcUtils.getJedisClient
    jedis.select(db)
    for (elem <- offsetRanges) {
      jedis.hset(elem.topic,elem.partition.toString,elem.untilOffset.toString)
    }
    jedis.close()
  }


  /**
    * 从Mysql中读取偏移量
    * @param groupId
    * @param topic
    * @return
    */
  def getOffset(groupId:String,topic:String):Map[TopicPartition,Long]={
    var offsetMap=Map[TopicPartition,Long]()

    val jedisClient: Jedis = JdbcUtils.getJedisClient

    val redisOffsetMap: util.Map[String, String] = jedisClient.hgetAll("offset:"+groupId+":"+topic)

    val offsetJsonObjList: List[JSONObject] = MysqlUtil.queryList("SELECT  group_id ,topic,partition_id  , topic_offset  FROM offset_2020 where group_id='"+groupId+"' and topic='"+topic+"'")

    jedisClient.close()
    if(offsetJsonObjList!=null&&offsetJsonObjList.size==0){
      null
    }else {

      val kafkaOffsetList: List[(TopicPartition, Long)] = offsetJsonObjList.map { offsetJsonObj  =>
        (new TopicPartition(offsetJsonObj.getString("topic"),offsetJsonObj.getIntValue("partition_id")), offsetJsonObj.getLongValue("topic_offset"))
      }
      kafkaOffsetList.toMap
    }
  }

}
