package com.lb.sparktest.dw

import java.lang
import java.time.LocalDate
import java.util.ResourceBundle

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.lb.sparktest.bean.{OrderInfo, ProvinceInfo, UserState}
import com.lb.util.{MyEsUtil, MyKafkaConsumer, MyKafkaSink, PhoenixUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}

import scala.collection.mutable.ListBuffer

object OrderInfoApp {
  private val bundle: ResourceBundle = ResourceBundle.getBundle("jdbc")

  def main(args: Array[String]): Unit = {
    val checkpoint = "/Users/longbei/IdeaProjects/sparkcal/sparktest/odsorder"
    val ssc: StreamingContext = StreamingContext.getOrCreate(checkpoint,()=>firstOrderStat(checkpoint))

    ssc.start()
    ssc.awaitTermination()
  }


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
    val kafkaDs: InputDStream[(String, String)] = MyKafkaConsumer.getKafkaStream(dbIndex,topic,ssc)


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
          for (i <- 1 until sortedLists.size) {
            sortedLists(i).if_first_order="0"
          }
          sortedLists
        } else {
          orderInfoes
        }
    }

    val orderWithProvinceDS: DStream[OrderInfo] = finalDS.transform(rdd => {
      val lists: List[JSONObject] = PhoenixUtil.queryList("select ID,NAME,REGION_ID,AREA_CODE from GMALL_PROVINCE_INFO")
      val provinceMap: Map[lang.Long, ProvinceInfo] = lists.map(jsonObj => {
        (jsonObj.getLong("ID"), jsonObj.toJavaObject(classOf[ProvinceInfo]))
      }).toMap
      val provinBC: Broadcast[Map[lang.Long, ProvinceInfo]] = ssc.sparkContext.broadcast(provinceMap)
      val orderInfoWithPro: RDD[OrderInfo] = rdd.mapPartitions(orderItr => {
        val proMap: Map[lang.Long, ProvinceInfo] = provinBC.value
        val list: List[OrderInfo] = orderItr.toList
        for (elem <- list) {
          val provinceInfo: ProvinceInfo = proMap.getOrElse(elem.province_id, null)
          if (provinceInfo != null) {
            elem.province_area_code = provinceInfo.area_code
            elem.province_name = provinceInfo.name
          }
        }
        list.toIterator
      })
      orderInfoWithPro
    })

    orderWithProvinceDS.count().print(1)
    val perfectDS: DStream[OrderInfo] = orderWithProvinceDS.mapPartitions(ordItr => {
      val ordList: List[OrderInfo] = ordItr.toList
      if (ordList.size > 0) {
        val uidlist: List[Long] = ordList.map(_.user_id)
        val sql = "select id,user_level,birthday,gender,age_group,gender_name from gmall_user_info where id in ('" + uidlist.mkString("','") + "')"
        val userJsonObjList: List[JSONObject] = PhoenixUtil.queryList(sql)
        val userJsonMap: Map[lang.Long, JSONObject] = userJsonObjList.map((userJsonObj => (userJsonObj.getLong("ID"), userJsonObj))).toMap
        for (elem <- ordList) {
          val userJsnObj: JSONObject = userJsonMap.getOrElse(elem.user_id, null)
          if (userJsnObj!=null){
            elem.user_age_group = userJsnObj.getString("AGE_GROUP")
            elem.user_gender = userJsnObj.getString("GENDER_NAME")
          }
        }
      }
      ordList.iterator
    })

    perfectDS.foreachRDD(rdd=>{
      val userStatRDD: RDD[UserState] = rdd.filter(_.if_first_order=="1").map(orderinfo=>UserState(orderinfo.user_id.toString,orderinfo.if_first_order))
      import org.apache.phoenix.spark._
      //字段顺序，参数个数必须一致
      userStatRDD.saveToPhoenix("user_state",Seq("USER_ID","IF_CONSUMED"),new Configuration,Some("node:2181"))
      val date: LocalDate = LocalDate.now()
      rdd.foreachPartition(orderItr=>{
        val list: List[OrderInfo] = orderItr.toList
        val orderInfoList: List[(String, OrderInfo)] = list.map(orderInfo=>(orderInfo.id.toString,orderInfo))
        //存储es

        MyEsUtil.executeIndexBulk("gmall_order_info_"+date.toString,list)

        //写dw
        for (elem <- orderInfoList) {
          MyKafkaSink.send("DW_ORDER_INFO",elem._1,JSON.toJSONString(elem._2,new SerializeConfig(true)))
        }
      })
      //周期性执行
      MyKafkaConsumer.saveOffsetToRedis(dbIndex,offsetRanges)
    })

//        ssc.checkpoint(checkpoint)
//        kafkaDs.checkpoint(Duration(5*10*1000))
    ssc
  }

}
