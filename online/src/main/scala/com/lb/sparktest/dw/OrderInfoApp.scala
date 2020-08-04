package com.lb.sparktest.dw

import java.lang
import java.util.ResourceBundle

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.lb.sparktest.bean.{OrderInfo, ProvinceInfo, UserState}
import com.lb.util.{MyEsUtil, MyKafkaConsumer, MyKafkaSink, PhoenixUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}

import scala.collection.mutable.ListBuffer

object OrderInfoApp {
  private val bundle: ResourceBundle = ResourceBundle.getBundle("jdbc")

  def firstOrderStat(checkpoint: String): StreamingContext = {

    val topic: String = "ODS_T_ORDER_INFO"
    val dbIndex = bundle.getString("dbIndex").toInt
//    val topic: String = bundle.getString("topic")

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
    val orderDS: DStream[OrderInfo] = transforDS.mapPartitions(orderItr => {
      val list: List[(String, String)] = orderItr.toList
      val jsonLists = new ListBuffer[OrderInfo]
      for (elem <- list) {
        val info: OrderInfo = JSON.parseObject(elem._2,classOf[OrderInfo])
        val createtime: Array[String] = info.create_time.split(" ")
        info.create_date = createtime(0)
        info.create_hour = createtime(1).split(":")(0)
        jsonLists += info
      }
      jsonLists.toIterator
    })

    val updateStateDS: DStream[OrderInfo] = orderDS.mapPartitions(orderItr => {
      val orderInfoOriginlist: List[OrderInfo] = orderItr.toList
      val condition: String = orderInfoOriginlist.map(_.user_id.toString).mkString("','")
      var sql = "select USER_ID,IF_CONSUMED from user_state where user_id in ('" + condition + "')"
      val userStateList: List[JSONObject] = PhoenixUtil.queryList(sql)

      //list 2 map
      val userStateMap: Map[String, String] = userStateList.map(userStateJson => {
        (userStateJson.getString("USER_ID"), userStateJson.getString("IF_CONSUMED"))
      }).toMap

      //遍历每一笔订单并更改状态
      for (elem <- orderInfoOriginlist) {
        val isConsumed: String = userStateMap.getOrElse(elem.user_id.toString, null)
        if (isConsumed!=null &&  "1".equals(isConsumed)) {//消费过
          elem.if_first_order = "0"
        } else{//没消费过
          elem.if_first_order = "1"
        }
      }

      orderInfoOriginlist.toIterator
    })

    val finalDS: DStream[OrderInfo] = updateStateDS.map(order => {
      (order.user_id, order)
    }).groupByKey().flatMap {
      case (uid, orders) =>
        val orderInfoes: List[OrderInfo] = orders.toList
        if (orderInfoes.size > 1) {
          val sortedLists: List[OrderInfo] = orderInfoes.sortWith((o1, o2) => o1.create_time.compareTo(o2.create_time) < 0)
          sortedLists
        } else {
          orderInfoes
        }
    }
    updateStateDS.cache()

    updateStateDS.print(10)
    val perfectDS: DStream[OrderInfo] = finalDS.transform(rdd => {
      val lists: List[JSONObject] = PhoenixUtil.queryList("select ID,NAME,REGION_ID,AREA_CODE from GMALL_PROVINCE_INFO")
      val provinceMap: Map[lang.Long, ProvinceInfo] = lists.map(jsonObj => (jsonObj.getLong("id"), jsonObj.toJavaObject(classOf[ProvinceInfo]))).toMap
      val provinBC: Broadcast[Map[lang.Long, ProvinceInfo]] = ssc.sparkContext.broadcast(provinceMap)
      val value: RDD[OrderInfo] = rdd.mapPartitions(orderItr => {
        val proBC: Map[lang.Long, ProvinceInfo] = provinBC.value
        for (elem <- orderDS) {
          /*elem
          val provinceInfo: ProvinceInfo = proBC.getOrElse(elem.province_id, null)
          if (provinceInfo != null) {
            elem.province_name = provinceInfo.name
            elem.province_area_code = provinceInfo.area_code
          }*/
        }
        orderItr
      })
      value
    })
    perfectDS.print(10)
    perfectDS.foreachRDD(rdd=>{
      val userStatRDD: RDD[UserState] = rdd.filter(_.if_first_order=="1").map(orderinfo=>UserState(orderinfo.user_id.toString,orderinfo.if_first_order))
      import org.apache.phoenix.spark._
      userStatRDD.saveToPhoenix("user_state",Seq("USER_ID","IF_CONSUMED"),new Configuration,Some("master:2181"))
      rdd.foreachPartition(orderItr=>{
        val orderInfoList: List[(String, OrderInfo)] = orderItr.toList.map(orderInfo=>(orderInfo.id.toString,orderInfo))
        //存储es
        MyEsUtil.executeIndexBulk("gmall_order_info_",orderInfoList)

        //写dw
        for (elem <- orderInfoList) {
          MyKafkaSink.send("DW_ORDER_INFO",elem._1,JSON.toJSONString(elem._2,new SerializeConfig(true)))
        }
      })
      //周期性执行
      MyKafkaConsumer.saveOffsetToRedis(dbIndex,offsetRanges)
    })

    ssc
  }

  def main(args: Array[String]): Unit = {
    val checkpoint = "/Users/longbei/IdeaProjects/sparkcal/sparktest/odappck"
    val ssc: StreamingContext = StreamingContext.getOrCreate(checkpoint,()=>firstOrderStat(checkpoint))

    ssc.start()
    ssc.awaitTermination()
  }

}
