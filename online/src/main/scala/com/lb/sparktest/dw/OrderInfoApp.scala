package com.lb.sparktest.dw

import java.util.ResourceBundle

import com.alibaba.fastjson.{JSON, JSONObject}
import com.lb.util.{MyKafkaConsumer, PhoenixUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}

import scala.collection.mutable.ListBuffer

object OrderInfoApp {
  private val bundle: ResourceBundle = ResourceBundle.getBundle("jdbc")

  def firstOrderStat(checkpoint: String): StreamingContext = {

    val topic: String = bundle.getString("topic")

    val conf: SparkConf = new SparkConf().setMaster("local[4]")
      .setAppName(getClass.getSimpleName)
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.kafka.maxRatePerPartition", "200")
      .set("spark.executor.instances", "2")
      .set("spark.default.parallelism", "5")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.streaming.concurrentJobs", "4")

    val ssc = new StreamingContext(conf,Seconds(10))
    val kafkaDs: InputDStream[(String, String)] = MyKafkaConsumer.getKafkaStream(topic,ssc)

    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val transforDS: DStream[(String, String)] = kafkaDs.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })
//    {"database":"gmall","table":"payment_info","type":"delete","ts":1596175213,"xid":2193,"xoffset":795,"data":{"id":5113,"out_trade_no":"866363926297812","order_id":3461,"user_id":96,"alipay_trade_no":"3377183623441392232922916988646873","total_amount":3882.00,"subject":"联想(Lenovo)拯救者Y7000 英特尔酷睿i7 2019新款 15.6英寸发烧游戏本笔记本电脑（i7-9750H 8GB 512GB SSD GTX1650 4G 高色域等1件商品","payment_type":"1103","payment_time":"2020-05-15 13:59:14"}}
    val orderDS: DStream[OrderInfo] = transforDS.mapPartitions(orderItr => {
      val list: List[(String, String)] = orderItr.toList
      val jsonLists = new ListBuffer[OrderInfo]
      for (elem <- list) {
        val info: OrderInfo = JSON.parseObject(elem._2,classOf[OrderInfo])
        jsonLists += info
      }
      jsonLists.toIterator
    })
    orderDS.mapPartitions(orderItr=>{
      val condition: String = orderItr.toList.map(_.user_id.toString).mkString(",")
      var sql = "select xxx from user_state where user_id in ("+condition+")"
      val userStateList: List[JSONObject] = PhoenixUtil.queryList(sql)
      userStateList.map(userStateJson=>{
        (userStateJson.getString("user_id")
        )
      })
      List(1,2,3).toIterator
    })
    kafkaDs.print(10)

    ssc
  }

  def main(args: Array[String]): Unit = {
    val checkpoint = ""
    val ssc: StreamingContext = StreamingContext.getOrCreate(checkpoint,()=>firstOrderStat(checkpoint))

    ssc.start()
    ssc.awaitTermination()
  }

}


case class OrderInfo(
                      id: Long,
                      province_id: Long,
                      order_status: String,
                      user_id: Long,
                      final_total_amount: Double,
                      benefit_reduce_amount: Double,
                      original_total_amount: Double,
                      feight_fee: Double,
                      expire_time: String,
                      create_time: String,
                      operate_time: String,
                      var create_date: String,
                      var create_hour: String,
                      var if_first_order:String,

                      var province_name:String,
                      var province_area_code:String,

                      var user_age_group:String,
                      var user_gender:String

                    )