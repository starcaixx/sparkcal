package com.lb

import java.lang
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}
import java.util.ResourceBundle

import com.alibaba.fastjson.{JSON, JSONObject}
import com.lb.sparktest.bean.DauInfo
import com.lb.util.{JdbcUtils, MyEsUtil, MyKafkaConsumer}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object RealTimeDauStat {
  private val bundle: ResourceBundle = ResourceBundle.getBundle("jdbc")

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Usage:Please input checkpoint path")
      System.exit(1)
    }

    val checkpoint = args(0)

    val streamingContext = StreamingContext.getOrCreate(checkpoint, () => getRealTimeDauCount(checkpoint))
    streamingContext.start()
    streamingContext.awaitTermination()
    streamingContext.stop()
  }

  /**
    * 只获取指定表名的数据记录
    *
    * @param rdd
    * @return
    */
  def filerOtherData(rdd: JSONObject): Boolean = {
    rdd != null && "airflowpy".equals(rdd.getString("database")) && "dag".equals(rdd.getString("table"))
    //    && "".equals(rdd.getString("type"))
  }


  def isJsonStr(rdd: (String, String)): Boolean = {
    try {
      val nObject: JSONObject = JSON.parseObject(rdd._2)
      return true
    } catch {
      case e: Exception => return false
    }
  }

  /**
    * 获取当天的活跃用户
    *
    * @param tuple
    * @return
    */
  def isBelongToday(tuple: (String, Int)): Boolean = {
    val dt: String = tuple._1.split(":")(1)
    val date: LocalDate = LocalDate.now()
    dt.equals(date)
  }

  def getRealTimeDauCount(checkpoint: String): StreamingContext = {
    val interval: Long = bundle.getString("processingInterval").toLong
    val topic: String = bundle.getString("topic")
    val dbIndex = bundle.getString("dbIndex").toInt
    val rediskeyexists: Int = bundle.getString("rediskeyexists").toInt

    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      Some(values.sum + state.getOrElse(0))
    }


    val conf = new SparkConf().setMaster("local[4]")
      .setAppName(getClass.getSimpleName)
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.kafka.maxRatePerPartition", "200")
      .set("spark.executor.instances", "2")
      .set("spark.default.parallelism", "5")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.streaming.concurrentJobs", "4") //这个参数?

    val ssc = new StreamingContext(conf, Seconds(interval))

    val kafkaDS: InputDStream[(String, String)] = MyKafkaConsumer.getKafkaStream(dbIndex,topic, ssc)

    var offsetRanges = Array[OffsetRange]()
    val jsonDS: DStream[JSONObject] = kafkaDS.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }).filter(rdd => isJsonStr(rdd)).map(rdd => {
      JSON.parseObject(rdd._2)
    }).filter(filerOtherData(_))

    jsonDS.print(2)

    val uidRdd: DStream[(String, lang.Long)] = jsonDS.map(json => {
      val ts: lang.Long = json.getLong("ts") * 1000
      val localDateTime: LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneId.systemDefault)
      (json.getJSONObject("data").getString("dag_id") + ":" + localDateTime.toLocalDate, ts)
    })

    uidRdd.print(3)
    //RDD去重

    val updateStateDS: DStream[(String, Int)] = uidRdd.map(tupl => (tupl._1, 1)).updateStateByKey(updateFunc)
    updateStateDS.print(5)
    val uv: DStream[Long] = updateStateDS.filter(isBelongToday(_)).map(_._1).count()

    uv.print()
    //    uv.foreachRDD(rdd=>{
    //      val longs: Array[Long] = rdd.collect()
    //      println(longs.length)
    //      MyKafkaConsumer.saveOffsetToRedis(dbIndex,offsetRanges)
    //    })

    //    result.print(10)
    val resultDS: DStream[(String, lang.Long)] = uidRdd.transform(rdd => {
      val logInfoRdd: RDD[(String, lang.Long)] = rdd.mapPartitions(uids => {
        val lists = new ListBuffer[(String, lang.Long)]
        val jedis: Jedis = JdbcUtils.getJedisClient
        val localDate: LocalDate = LocalDate.now()
        for (elem <- uids) {
          val key = "dau:" + localDate
          val isNew: lang.Long = jedis.sadd(key, elem._1)
          if (isNew == 1) {
            lists += elem
          }
          jedis.expire(key, rediskeyexists)
        }
        jedis.close()
        lists.toIterator
      })
      /*val logInfoRdd: RDD[String] = rdd.reduceByKey(_ + _).map(_._1).mapPartitions(uids => {
        val lists = new ListBuffer[String]
        val jedis: Jedis = JdbcUtils.getJedisClient
        val localDate: LocalDate = LocalDate.now()
        for (elem <- uids) {
          val key = "dau:" + localDate
          val isNew: lang.Long = jedis.sadd(key, elem)
          if (isNew == 1) {
            lists += elem
          }
          jedis.expire(key, rediskeyexists)
        }
        jedis.close()
        lists.toIterator
      })*/
      logInfoRdd
    })

    val dauInfoDS: DStream[DauInfo] = resultDS.map(tup => {
      val fields: Array[String] = tup._1.split(":")
      DauInfo(fields(0), fields(1), tup._2)
    })

    dauInfoDS.foreachRDD(rdd => {
      println("rdd run....")
      //write2es
      rdd.foreachPartition { dauInfoItr =>
        val dauInfoWithIdList: List[(String, DauInfo)] = dauInfoItr.toList.map(dauInfo => (dauInfo.dt + "_" + dauInfo.mid, dauInfo))
        MyEsUtil.executeIndexBulk("lb_dau_info_" + dauInfoItr, dauInfoWithIdList)
      }

      MyKafkaConsumer.saveOffsetToRedis(dbIndex, offsetRanges)
    })

    //利用redis去重
    // key: dau:dt uid

    ssc.checkpoint(checkpoint)
    kafkaDS.checkpoint(Seconds(interval * 5))
    ssc
  }

}
