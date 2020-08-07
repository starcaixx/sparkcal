package com.lb.sparktest.dws

import java.util.ResourceBundle

import com.alibaba.fastjson.JSON
import com.lb.sparktest.bean.{OrderDetail, OrderInfo}
import com.lb.util.MyKafkaConsumer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Durations, Seconds, StreamingContext}

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
      .setMaster("local[4]")
    val dbIndex: Int = bundle.getString("localDbIndex").toInt
    val topicOrderDetail = "DW_ORDER_DETAIL"
    val topicOrderInfo = "DW_ORDER_INFO"
    val conf: SparkConf = new SparkConf().setAppName(getClass.getSimpleName)
      .setMaster("local[4]")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.kafka.maxRatePerPartition", "200")
      .set("spark.executor.instances", "2")
      .set("spark.default.parallelism", "5")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.streaming.concurrentJobs", "4")
    val ssc = new StreamingContext(conf, Seconds(10))
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

    //每隔一分钟查看过去一小时的数据情况 要用到窗口操作  普通版本
    //每隔10s通过func计算过去20s的数据   滑动间隔和窗口长度必须是批处理时间的整数倍
//    detailDS.reduceByKeyAndWindow((o1,o2)=>{o1+o2},Durations.seconds(20),Durations.seconds(10))

    //优化机制 必须设置checkpoint，不设置报错
    //每隔10s通过fun1聚合进来，通过fun2减少出窗口的数据，计算过去20s的数据
//    detailDS.reduceByKeyAndWindow((o1,o2)=>{o1+o2},(o1,o2)=>{o1-o2},Seconds(20),Seconds(10))

     val orderJoinedDS: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoDS.join(detailDS)

    orderJoinedDS.print(10)
    ssc
  }
}
