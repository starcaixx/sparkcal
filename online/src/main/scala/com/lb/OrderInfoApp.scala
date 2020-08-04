package com.lb

import java.util.ResourceBundle

import com.alibaba.fastjson.{JSON, JSONObject}
import com.lb.sparktest.bean.{OrderInfo, UserState}
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
    val userStateDS: DStream[UserState] = orderedInfoDS.filter(_.if_first_order == "1").map(orderInfo => UserState(orderInfo.id.toString, orderInfo.if_first_order))
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
    //在一个批次内 第一笔如果是首单 那么本批次的该用户其他单据改为非首单
    val orderInfoFinalDstream: DStream[OrderInfo]

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
