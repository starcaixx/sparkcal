package com.lb.sparktest.dim

import java.util.ResourceBundle

import com.alibaba.fastjson.{JSON, JSONObject}
import com.lb.sparktest.bean.SkuInfo
import com.lb.util.{MyKafkaConsumer, PhoenixUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}

object SkuInfoApp {
  private val bundle: ResourceBundle = ResourceBundle.getBundle("jdbc")
  def main(args: Array[String]): Unit = {
    val checkpoint = "/Users/longbei/IdeaProjects/sparkcal/sparktest/skuinfo"
    val ssc: StreamingContext = StreamingContext.getOrCreate(checkpoint,()=>spuInfoSync(checkpoint))
    ssc.start()
    ssc.awaitTermination()
  }

  def spuInfoSync(checkpoint: String): StreamingContext = {
    val dbIndex: Int = bundle.getString("localDbIndex").toInt
    val topic = "ODS_T_SKU_INFO"
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
    val skuInfoDS: DStream[SkuInfo] = transFormDS.map(record => {
      JSON.parseObject(record._2, classOf[SkuInfo])
    })

    val newSkuInfoDS: DStream[SkuInfo] = skuInfoDS.transform(rdd => {
      if (rdd.count() > 0) {
        val categorySql = "select id,name from gmall_base_category3"
        val categoryList: List[JSONObject] = PhoenixUtil.queryList(categorySql)
        val categoryMap: Map[String, JSONObject] = categoryList.map(jsonObj => (jsonObj.getString("ID"), jsonObj)).toMap

        val tmpSql = "select id,tm_name from gmall_base_trademark"
        val tmpMap: Map[String, JSONObject] = PhoenixUtil.queryList(tmpSql).map(jsonObj => (jsonObj.getString("ID"), jsonObj)).toMap

        val spuSql = "select id,spu_name from gmall_spu_info"
        val spuMap: Map[String, JSONObject] = PhoenixUtil.queryList(spuSql).map(jsonObj => (jsonObj.getString("ID"), jsonObj)).toMap

        val dimList: List[Map[String, JSONObject]] = List[Map[String, JSONObject]](categoryMap, tmpMap, spuMap)
        val dimBC: Broadcast[List[Map[String, JSONObject]]] = ssc.sparkContext.broadcast(dimList)
        rdd.mapPartitions(skuItr => {
          val dim: List[Map[String, JSONObject]] = dimBC.value
          val cateMap: Map[String, JSONObject] = dim(0)
          val tmMap: Map[String, JSONObject] = dim(1)
          val spMap: Map[String, JSONObject] = dim(2)
          val skuLists: List[SkuInfo] = skuItr.toList
          for (elem <- skuLists) {
            val cate3Obj: JSONObject = cateMap.getOrElse(elem.category3_id, null)
            if (cate3Obj != null) {
              elem.category3_name = cate3Obj.getString("NAME")
            }
            val tmObj: JSONObject = tmMap.getOrElse(elem.tm_id, null)
            if (tmObj != null) {
              elem.tm_name = tmObj.getString("TM_NAME")
            }
            val spuObj: JSONObject = spMap.getOrElse(elem.spu_id, null)
            if (spuObj != null) {
              elem.spu_name = spuObj.getString("SPU_NAME")
            }
          }
          skuLists.toIterator
        })
      } else {
        rdd
      }
    })
    newSkuInfoDS.foreachRDD(rdd=>{
      import org.apache.phoenix.spark._
      rdd.saveToPhoenix("GMALL_SKU_INFO",Seq("ID","SPU_ID","PRICE","SKU_NAME","TM_ID","CATEGORY3_ID","CREATE_TIME","CATEGORY3_NAME","SPU_NAME","TM_NAME"),new Configuration(),Some("node:2181"))
      MyKafkaConsumer.saveOffsetToRedis(dbIndex,offsetRanges)
    })

    ssc
  }

}
