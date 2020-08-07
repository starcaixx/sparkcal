package com.lb.sparktest.dw

import java.util.ResourceBundle

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.lb.sparktest.bean.OrderDetail
import com.lb.util.{MyKafkaConsumer, MyKafkaSink, PhoenixUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OrderDetailApp {
  private val bundle: ResourceBundle = ResourceBundle.getBundle("jdbc")


  def main(args: Array[String]): Unit = {
    val checkpoint = "/Users/longbei/IdeaProjects/sparkcal/sparktest/orderdetail"
    val ssc: StreamingContext = StreamingContext.getOrCreate(checkpoint,()=>orderDetailSync(checkpoint))

    ssc.start()
    ssc.awaitTermination()
  }

  def orderDetailSync(checkpoint: String): StreamingContext = {

    val interval = bundle.getString("processingInterval").toInt
    val topic: String = "ODS_T_ORDER_DETAIL"
    val dbIndex = bundle.getString("localDbIndex").toInt
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
    val tranFormDS: DStream[(String, String)] = kafkaDS.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })
    val orderDetailDS: DStream[OrderDetail] = tranFormDS.map(record => {
      JSON.parseObject(record._2, classOf[OrderDetail])
    })
    val orderWithSkuDS: DStream[OrderDetail] = orderDetailDS.mapPartitions(detailItr => {
      val orderDetailList: List[OrderDetail] = detailItr.toList
      val sku_ids: List[Long] = orderDetailList.map(_.sku_id)
      val sql = "select id ,tm_id,spu_id,category3_id,tm_name ,spu_name,category3_name  from gmall_sku_info  where id in ('" + sku_ids.mkString("','") + "')"
      val skuJsonObj: List[JSONObject] = PhoenixUtil.queryList(sql)
      val skuJsonMap: Map[Long, JSONObject] = skuJsonObj.map(skuJsonObj => (skuJsonObj.getLongValue("ID"), skuJsonObj)).toMap
      for (elem <- orderDetailList) {
        val skuObj: JSONObject = skuJsonMap.getOrElse(elem.sku_id, null)
        if (skuObj != null) {
          elem.spu_id = skuObj.getLong("SPU_ID")
          elem.spu_name = skuObj.getString("SPU_NAME")
          elem.tm_id = skuObj.getLong("TM_ID")
          elem.tm_name = skuObj.getString("TM_NAME")
          elem.category3_id = skuObj.getLong("CATEGORY3_ID")
          elem.category3_name = skuObj.getString("CATEGORY3_NAME")
        }
      }
      orderDetailList.toIterator
    })
//    orderWithSkuDS.cache()
    orderWithSkuDS.print(10)

    orderWithSkuDS.foreachRDD(rdd=>{
      rdd.foreachPartition(orderDetailItr=>{
        val orderList: List[OrderDetail] = orderDetailItr.toList
        for (elem <- orderList) {
//          MyKafkaSink.send("DW_ORDER_DETAIL",elem.id.toString,JSON.toJSONString(elem,new SerializeConfig(true)))
          //这里根据orderid作为key可以将相同订单的明细进入同一个topic的一个分区中，方便后续处理时减少shuffle操作
          MyKafkaSink.send("DW_ORDER_DETAIL",elem.order_id.toString,JSON.toJSONString(elem,new SerializeConfig(true)))
        }
      })
      MyKafkaConsumer.saveOffsetToRedis(dbIndex,offsetRanges)
    })

    ssc
  }
}
