package com.lb

import java.util.ResourceBundle

import com.alibaba.fastjson.{JSON, JSONObject}
import com.lb.util.{MyKafkaConsumer, PhoenixUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OrderInfoApp {
  private val bundle: ResourceBundle = ResourceBundle.getBundle("jdbc")

  def main(args: Array[String]): Unit = {

    val checkPoint = ""
    val ssc: StreamingContext = StreamingContext.getOrCreate(checkPoint, () => currentDayConsumeCount(checkPoint))

    ssc.start()
    ssc.awaitTermination()
  }

  def currentDayConsumeCount(checkpoint: String): StreamingContext = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(getClass.getSimpleName)
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.kafka.maxRatePerPartition", "200")
      .set("spark.executor.instances", "2")
      .set("spark.default.parallelism", "4")
      .set("spark.sql.shuffle.partitions", "4")

    val interval: Long = bundle.getString("interval").toLong
    val groupId: String = bundle.getString("groupid")

    val topic: String = bundle.getString("topic")
    val ssc = new StreamingContext(conf, Seconds(interval))

    ssc.sparkContext.setLogLevel("error")
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val kafkaDS: InputDStream[(String, String)] = MyKafkaConsumer.getKafkaStream(topic, ssc)
    val transformDS: DStream[(String, String)] = kafkaDS.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })
    val orderInfoDS: DStream[OrderInfo] = transformDS.map(record => {
      val jsonString: String = record._2
      val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
      val createTimeArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = createTimeArr(0)
      orderInfo.create_hour = createTimeArr(1).split(":")(0)

      orderInfo
    })
    val orderWithIsOrderedDS: DStream[OrderInfo] = orderInfoDS.mapPartitions(orderInfoItr => {
      val orderInfoList: List[OrderInfo] = orderInfoItr.toList
      if (orderInfoList.size > 0) {
        val userIds: String = orderInfoList.map("'" + _.user_id + "'").mkString(",")
        val userStateList: List[JSONObject] = PhoenixUtil.queryList("select user_id,if_consumed from GMALL_USER_STATE where user_id in (" + userIds + ")")
        val userIsOrderedMap: Map[Long, String] = userStateList.map(userStateObj => (userStateObj.getLong("USER_ID").toLong, userStateObj.getString("IF_ORDERED"))).toMap
        for (orderInfo <- orderInfoList) {
          val isOrdered: String = userIsOrderedMap.getOrElse(orderInfo.user_id, "0")
          if ("1".equals(isOrdered)) {
            orderInfo.if_first_order = "0"
          } else {
            orderInfo.if_first_order = "1"
          }
        }
      } else {
        println("it's imposible")
      }
      orderInfoList.toIterator
    })
    val groupByUserDS: DStream[(Long, Iterable[OrderInfo])] = orderWithIsOrderedDS.map(orderInfo => {
      (orderInfo.user_id, orderInfo)
    }).groupByKey()
    val orderedInfoDS: DStream[OrderInfo] = groupByUserDS.flatMap {
      case (userId, orderInfoTtr) =>
        val orderList: List[OrderInfo] = orderInfoTtr.toList
        if (orderList.size > 1) {
          val sortedOrderList: List[OrderInfo] = orderList.sortWith((o1, o2) => o1.create_time.compareTo(o2.create_time) < 0)
          if ("1".equals(sortedOrderList(0).if_first_order)) {
            for (i <- 1 to sortedOrderList.size - 1) {
              sortedOrderList(i).if_first_order = "0"
            }
          }
          sortedOrderList
        } else {
          orderList
        }
    }
    orderedInfoDS.cache()
    val userStateDS: DStream[UserState] = orderedInfoDS.filter(_.if_first_order == "1").map(orderInfo => UserState(orderInfo.id, orderInfo.if_first_order))
    userStateDS.foreachRDD(rdd => {
      //      rdd.saveToPhenix()
    })
    orderInfoDS.print(100)

    //开启检查点
    ssc.checkpoint(checkpoint)
    //批处理时间的5-10倍
    kafkaDS.checkpoint(Seconds(interval * 10))
    //保存到文件
    //	resultDStream.saveAsTextFiles("")

    ssc
  }
}

/*
package com.atguigu.gmall2020.realtime.app.o2d

object OrderInfoApp {


  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("order_info_app")

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val groupId = "GMALL_ORDER_INFO_CONSUMER"
    val topic = "ODS_T_ORDER_INFO"

    //从redis读取偏移量
    val orderOffsets: Map[TopicPartition, Long] = OffsetManager.getOffset(groupId, topic)

    //根据偏移起始点获得数据
    //判断如果之前没有在redis保存，则从kafka最新加载数据
    var orderInfoInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (orderOffsets != null && orderOffsets.size > 0) {
      orderInfoInputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, orderOffsets, groupId)
    } else {
      orderInfoInputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    //获得偏移结束点
    var orderInfoOffsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val orderInfoInputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = orderInfoInputDstream.transform { rdd =>
      orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    val orderInfoDstream: DStream[OrderInfo] = orderInfoInputGetOffsetDstream.map { record =>
      val jsonString: String = record.value()
      val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])

      val createTimeArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = createTimeArr(0)
      orderInfo.create_hour = createTimeArr(1).split(":")(0)

      orderInfo
    }


    val orderWithIfFirstDstream: DStream[OrderInfo] = orderInfoDstream.mapPartitions { orderInfoItr =>
      val orderInfoList: List[OrderInfo] = orderInfoItr.toList
      if (orderInfoList.size > 0) {
        //针对分区中的订单中的所有客户 进行批量查询
        val userIds: String = orderInfoList.map("'" + _.user_id + "'").mkString(",")

        val userStateList: List[JSONObject] = PhoenixUtil.queryList("select user_id,if_consumed from GMALL0919_USER_STATE where user_id in (" + userIds + ")")
        // [{USERID:123, IF_ORDERED:1 },{USERID:2334, IF_ORDERED:1 },{USERID:4355, IF_ORDERED:1 }]
        // 进行转换 把List[Map] 变成Map
        val userIfOrderedMap: Map[Long, String] = userStateList.map(userStateJsonObj => (userStateJsonObj.getLong("USER_ID").toLong, userStateJsonObj.getString("IF_ORDERED"))).toMap
        //{123:1,2334:1,4355:1}
        //进行判断 ，打首单表情
        for (orderInfo <- orderInfoList) {
          val ifOrderedUser: String = userIfOrderedMap.getOrElse(orderInfo.user_id, "0") //
          //是下单用户不是首单   否->首单
          if (ifOrderedUser == "1") {
            orderInfo.if_first_order = "0"
          } else {
            orderInfo.if_first_order = "1"
          }
        }
        orderInfoList.toIterator
      } else {
        orderInfoItr
      }

    }


    //在一个批次内 第一笔如果是首单 那么本批次的该用户其他单据改为非首单
    // 以userId 进行分组
    val groupByUserDstream: DStream[(Long, Iterable[OrderInfo])] = orderWithIfFirstDstream.map(orderInfo => (orderInfo.user_id, orderInfo)).groupByKey()

    val orderInfoFinalDstream: DStream[OrderInfo] = groupByUserDstream.flatMap { case (userId, orderInfoItr) =>
      val orderList: List[OrderInfo] = orderInfoItr.toList
      //
      if (orderList.size > 1) { //   如果在这个批次中这个用户有多笔订单
        val sortedOrderList: List[OrderInfo] = orderList.sortWith((orderInfo1, orderInfo2) => orderInfo1.create_time < orderInfo2.create_time)
        if (sortedOrderList(0).if_first_order == "1") { //排序后，如果第一笔订单是首单，那么其他的订单都取消首单标志
          for (i <- 1 to sortedOrderList.size - 1) {
            sortedOrderList(i).if_first_order = "0"
          }
        }
        sortedOrderList
      } else {
        orderList
      }
    }

    orderInfoFinalDstream.cache()

    //同步到userState表中  只有标记了 是首单的用户 才需要同步到用户状态表中
    val userStateDstream: DStream[UserState] = orderInfoFinalDstream.filter(_.if_first_order == "1").map(orderInfo => UserState(orderInfo.id, orderInfo.if_first_order))
    userStateDstream.foreachRDD { rdd =>
      rdd.saveToPhoenix("GMALL2020_USER_STATE", Seq("USER_ID", "IF_CONSUMED"), new Configuration(), Some("hadoop1,hadoop2,hadoop3:2181"))
    }

    orderInfoFinalDstream.print(1000)
////////////////////////////////////////////////////////////
//////////////////////合并维表代码//////////////////////
////////////////////////////////////////////////////////////
    val orderInfoFinalWithProvinceDstream: DStream[OrderInfo] = orderInfoFinalDstream.transform { rdd =>
      val provinceJsonObjList: List[JSONObject] = PhoenixUtil.queryList("select id,name,region_id ,area_code from GMALL2020_PROVINCE_INFO  ")

      val provinceMap: Map[Long, ProvinceInfo] = provinceJsonObjList.map { jsonObj => (jsonObj.getLong("id").toLong, jsonObj.toJavaObject(classOf[ProvinceInfo])) }.toMap
      val provinceMapBC: Broadcast[Map[Long, ProvinceInfo]] = ssc.sparkContext.broadcast(provinceMap)

      val orderInfoWithProvinceRDD: RDD[OrderInfo] = rdd.map { orderInfo =>
        val provinceMap: Map[Long, ProvinceInfo] = provinceMapBC.value
        val provinceInfo: ProvinceInfo = provinceMap.getOrElse(orderInfo.province_id, null)
        if (provinceInfo != null) {
          orderInfo.province_name = provinceInfo.name
          orderInfo.province_area_code = provinceInfo.area_code
        }
        orderInfo
      }
      orderInfoWithProvinceRDD
    }

////////////////////////////////////////////////////////////
//////////////////////最终存储代码//////////////////////
////////////////////////////////////////////////////////////
    orderInfoFinalWithProvinceDstream.foreachRDD{rdd=>
      rdd.cache()
      //存储用户状态
      val userStateRdd: RDD[UserState] = rdd.filter(_.if_first_order == "1").map(orderInfo => UserState(orderInfo.id, orderInfo.if_first_order))
      userStateRdd.saveToPhoenix("GMALL2020_USER_STATE", Seq("USER_ID", "IF_CONSUMED"), new Configuration(), Some("hadoop1,hadoop2,hadoop3:2181"))

      rdd.foreachPartition { orderInfoItr =>

        val orderInfoList: List[(String, OrderInfo)] = orderInfoItr.toList.map(orderInfo => (orderInfo.id.toString, orderInfo))
        val dateStr: String = new SimpleDateFormat("yyyyMMdd").format(new Date())
        MyEsUtil.bulkInsert(orderInfoList, "gmall2020_order_info_" + dateStr)

for ((id,orderInfo) <- orderInfoList ) {
  MyKafkaSink.send("DW_ORDER_INFO",id,JSON.toJSONString(orderInfo,new SerializeConfig(true)))
}

      }
      OffsetManager.saveOffset(groupId, topic, orderInfoOffsetRanges)


    }


    ssc.start()
    ssc.awaitTermination()

  }


}

 */

case class UserState(id: Long, if_first_order: String)

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
                      var if_first_order: String,
                      var province_name: String,
                      var province_area_code: String,
                      var user_age_group: String,
                      var user_gender: String
                    )