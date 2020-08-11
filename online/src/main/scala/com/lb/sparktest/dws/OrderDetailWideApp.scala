package com.lb.sparktest.dws

import java.lang
import java.util.ResourceBundle

import com.alibaba.fastjson.JSON
import com.lb.sparktest.bean.{OrderDetail, OrderDetailWide, OrderInfo}
import com.lb.util.{JdbcUtils, MyKafkaConsumer}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Durations, Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.immutable.ListSet
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object OrderDetailWideApp {
  private val bundle: ResourceBundle = ResourceBundle.getBundle("jdbc")

  def main(args: Array[String]): Unit = {
    val checkpoint = "/Users/longbei/IdeaProjects/sparkcal/sparktest/orderjoin"
    val ssc: StreamingContext = StreamingContext.getOrCreate(checkpoint, () => orderDetailWide(checkpoint))
    ssc.start()
    ssc.awaitTermination()

  }

  def orderDetailWide(checkpoint: String): StreamingContext = {
    new SparkConf().setAppName(getClass.getSimpleName)
      .setMaster("local[2]")
    val dbIndex: Int = bundle.getString("localDbIndex").toInt
    val topicOrderDetail = "DW_ORDER_DETAIL"
    val topicOrderInfo = "DW_ORDER_INFO"
    val conf: SparkConf = new SparkConf().setAppName(getClass.getSimpleName)
      .setMaster("local[2]")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.kafka.maxRatePerPartition", "200")
      .set("spark.executor.instances", "2")
      .set("spark.default.parallelism", "5")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.streaming.concurrentJobs", "4")
    val ssc = new StreamingContext(conf, Seconds(5))
    val kafkaOrderDetailDS: InputDStream[(String, String)] = MyKafkaConsumer.getKafkaStream(dbIndex, topicOrderDetail, ssc)
    val kafkaOrderInfoDS: InputDStream[(String, String)] = MyKafkaConsumer.getKafkaStream(dbIndex, topicOrderInfo, ssc)

    var orderDetailOffsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    var orderInfoOffsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val detailTransFormDS: DStream[(String, String)] = kafkaOrderDetailDS.transform(rdd => {
      orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })
    val infoTransFormDS: DStream[(String, String)] = kafkaOrderInfoDS.transform(rdd => {
      orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })

    val detailDS: DStream[(Long, OrderDetail)] = detailTransFormDS.map(record => {
      val detail: OrderDetail = JSON.parseObject(record._2, classOf[OrderDetail])
      (detail.order_id, detail)
    })

    val orderInfoDS: DStream[(Long, OrderInfo)] = infoTransFormDS.map(record => {
      val orderInfo: OrderInfo = JSON.parseObject(record._2, classOf[OrderInfo])
      (orderInfo.id, orderInfo)
    })

    orderInfoDS.print(10)
    val distinctDS: DStream[(Long, OrderInfo)] = orderInfoDS.reduceByKey((o1, o2) => {
      if (o1.operate_time!=null && o2.operate_time!=null && o1.operate_time.compareTo(o2.operate_time) < 0) {
        o2
      } else {
        o1
      }
    })

    //窗口开太大，处理数据压力比较大,产生大量冗余数据，开太小，容易出现数据丢失
    val orderInfoWindowDS: DStream[(Long, OrderInfo)] = distinctDS.window(Seconds(10),Seconds(5))
    val orderDetailWindwoDS: DStream[(Long, OrderDetail)] = detailDS.window(Seconds(10),Seconds(5))

    orderInfoWindowDS.print(100)
    //每隔一分钟查看过去一小时的数据情况 要用到窗口操作  普通版本
    //每隔10s通过func计算过去20s的数据   滑动间隔和窗口长度必须是批处理时间的整数倍
//    detailDS.reduceByKeyAndWindow((o1,o2)=>{o1+o2},Durations.seconds(20),Durations.seconds(10))

    //优化机制 必须设置checkpoint，不设置报错
    //每隔10s通过fun1聚合进来，通过fun2减少出窗口的数据，计算过去20s的数据
//    detailDS.reduceByKeyAndWindow((o1,o2)=>{o1+o2},(o1,o2)=>{o1-o2},Seconds(20),Seconds(10))

    val orderJoinedDS: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWindowDS.join(orderDetailWindwoDS)
    orderJoinedDS.count().print(1)
    orderJoinedDS.print(5)

    val orderDetailWideDS: DStream[OrderDetailWide] = orderJoinedDS.map {
      case (orderId, (orderInfo, orderDetail)) => new OrderDetailWide(orderInfo, orderDetail)
    }
    //去重
    val filterOrderWideDS: DStream[OrderDetailWide] = orderDetailWideDS.mapPartitions(orderWideItr => {
//      val jedis: Jedis = JdbcUtils.getJedisClient
      val orderWide: List[OrderDetailWide] = orderWideItr.toList

      val orderWideList: ListBuffer[OrderDetailWide] = ListBuffer[OrderDetailWide]()
      val orderWideMap = new mutable.HashMap[String,OrderDetailWide]()
      //这块需要注意的一点，可能同一批次数据有同一条记录的多条数据，要取哪些记录或者以哪条记录为准需要判断
      for (elem <- orderWide) {
        //redis type key value expire
//        val key = "order_wide:order_id:" + elem.order_id
//        val isSucc: lang.Long = jedis.sadd(key, elem.order_detail_id.toString)
//        jedis.expire(key, 60 * 10)
        //这里先局部去重，下一步再通过redis整体去重
        if (!orderWideMap.contains(elem.order_detail_id.toString)){
          orderWideMap.put(elem.order_detail_id.toString,elem)
        }else{
          val tmpOrdWide: OrderDetailWide = orderWideMap.get(elem.order_detail_id.toString).get
          if(tmpOrdWide.create_time<elem.create_time){
            orderWideMap.put(elem.order_detail_id.toString,elem)
          }
        }
//        if (isSucc == 1) {
//          orderWideList += elem
//        }
      }
//      jedis.close()
//      orderWideList.toIterator
      orderWideMap.map(_._2).toIterator
    })
    filterOrderWideDS.print(5)
    filterOrderWideDS.count().print(1)

    /*filterOrderWideDS.mapPartitions(ordWideItr=>{
      val list: List[OrderDetailWide] = ordWideItr.toList
      for (elem <- list) {

      }
      null
    })*/

    filterOrderWideDS.map(ordWide=>{
      (ordWide.order_id,ordWide.final_total_amount,ordWide.original_total_amount,ordWide.sku_price,ordWide.sku_num,ordWide.final_detail_amount)
    }).print(100)
    filterOrderWideDS.foreachRDD(rdd=>{
      println(rdd.collect())
      MyKafkaConsumer.saveOffsetToRedis(dbIndex,orderDetailOffsetRanges)
      MyKafkaConsumer.saveOffsetToRedis(dbIndex,orderInfoOffsetRanges)
    })

    ssc
  }
}
