package com.lb

object OrderDetailWideApp {

  def main(args: Array[String]): Unit = {
  }

  /*
      val sparkConf: SparkConf = new SparkConf().setAppName("dws_order_detail_wide_app").setMaster("local[*]")
      val ssc = new StreamingContext(sparkConf, Seconds(5))
      val orderInfoTopic = "DW_ORDER_INFO"
      val orderDetailTopic = "DW_ORDER_DETAIL"
      val groupId = "dws_order_detail_wide_consumer"
      val orderInfoOffsets: Map[TopicPartition, Long] = OffsetManager.getOffset(groupId, orderInfoTopic)
      val orderDetailOffsets: Map[TopicPartition, Long] = OffsetManager.getOffset(groupId, orderDetailTopic)


      val orderInputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(orderInfoTopic, ssc, orderInfoOffsets, groupId)

      val orderDetailInputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(orderDetailTopic, ssc, orderDetailOffsets, groupId)

      var orderInfoOffsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
      val orderInputNDstream: DStream[ConsumerRecord[String, String]] = orderInputDstream.transform { rdd =>
        orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
      var orderDetailOffsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
      val orderDetailInputNDstream: DStream[ConsumerRecord[String, String]] = orderDetailInputDstream.transform { rdd =>
        orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }


      //把订单和订单明细 转换为 case class的流
      val orderInfoDstream: DStream[OrderInfo] = orderInputNDstream.map { record =>
        val jsonString: String = record.value()
        val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
        orderInfo
      }

      val orderDetailDstream: DStream[OrderDetail] = orderDetailInputNDstream.map(record => JSON.parseObject(record.value, classOf[OrderDetail]))

      val orderInfoWinDstream: DStream[OrderInfo] = orderInfoDstream.window(Seconds(15), Seconds(5))
      val orderDetailWinDstream: DStream[OrderDetail] = orderDetailDstream.window(Seconds(15), Seconds(5))
      orderInfoWinDstream.cache()
      orderDetailWinDstream.cache()
      // orderInfoWinDstream.print(1000)
      //  orderDetailWinDstream.print(1000)


      // orderinfo 和 orderDetail 的双流join
      val orderInfoWithKeyDstream: DStream[(Long, OrderInfo)] = orderInfoWinDstream.map(orderInfo => (orderInfo.id, orderInfo))
      val orderDetailWithKeyDstream: DStream[(Long, OrderDetail)] = orderDetailWinDstream.map(orderDetail => (orderDetail.order_id, orderDetail))

      val orderJoinDstream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithKeyDstream.join(orderDetailWithKeyDstream)

      val orderDetailWideDstream: DStream[OrderDetailWide] = orderJoinDstream.map { case (id, (orderInfo, orderDetail)) => new OrderDetailWide(orderInfo, orderDetail) }


      //去重
      val orderDetailWideFilteredDstream: DStream[OrderDetailWide] = orderDetailWideDstream.transform { rdd =>
        println("前：" + rdd.count())
        val logInfoRdd: RDD[OrderDetailWide] = rdd.mapPartitions { orderDetailWideItr =>
          val jedis: Jedis = RedisUtil.getJedisClient
          val orderDetailWideFilteredList = new ListBuffer[OrderDetailWide]
          val orderDetailWideList: List[OrderDetailWide] = orderDetailWideItr.toList

          println(orderDetailWideList.map(orderDetailWide => orderDetailWide.order_id).mkString(","))
          for (orderDetailWide <- orderDetailWideList) {

            val orderDetailWideKey = "order_detail_wide:" + orderDetailWide.dt
            val ifFirst: lang.Long = jedis.sadd(orderDetailWideKey, orderDetailWide.order_detail_id.toString)
            if (ifFirst == 1L) {
              orderDetailWideFilteredList += orderDetailWide
            }
          }
          jedis.close()
          orderDetailWideFilteredList.toIterator
        }
        logInfoRdd.cache()
        println("后：" + logInfoRdd.count())
        logInfoRdd
      }
      orderDetailWideFilteredDstream.map(orderwide=>(orderwide.order_id,orderwide.final_total_amount,orderwide.original_total_amount,  orderwide.sku_price,orderwide.sku_num,orderwide.final_detail_amount)).print(1000)
      ssc.start()
      ssc.awaitTermination()
    }*/

}

/*
case class OrderDetailWide(
                            var order_detail_id:Long =0L,
                            var order_id: Long=0L,
                            var order_status:String=null,
                            var create_time:String=null,
                            var user_id: Long=0L,
                            var sku_id: Long=0L,
                            var sku_price: Double=0D,
                            var sku_num: Long=0L,
                            var sku_name: String=null,
                            var benefit_reduce_amount:Double =0D ,
                            var original_total_amount:Double =0D ,
                            var feight_fee:Double=0D,
                            var final_total_amount: Double =0D ,
                            var final_detail_amount:Double=0D,

                            var if_first_order:String=null,

                            var province_name:String=null,
                            var province_area_code:String=null,

                            var user_age_group:String=null,
                            var user_gender:String=null,

                            var dt:String=null,

                            var spu_id: Long=0L,
                            var tm_id: Long=0L,
                            var category3_id: Long=0L,
                            var spu_name: String=null,
                            var tm_name: String=null,
                            var category3_name: String=null


                          )
{
  def this(orderInfo:OrderInfo,orderDetail: OrderDetail) {
    this
    mergeOrderInfo(orderInfo)
    mergeOrderDetail(orderDetail)

  }

  def mergeOrderInfo(orderInfo:OrderInfo): Unit ={
    if(orderInfo!=null){
      this.order_id=orderInfo.id
      this.order_status=orderInfo.order_status
      this.create_time=orderInfo.create_time
      this.dt=orderInfo.create_date

      this.benefit_reduce_amount  =orderInfo.benefit_reduce_amount
      this.original_total_amount  =orderInfo.original_total_amount
      this.feight_fee =orderInfo.feight_fee
      this.final_total_amount  =orderInfo.final_total_amount


      this.province_name=orderInfo.province_name
      this.province_area_code=orderInfo.province_area_code

      this.user_age_group=orderInfo.user_age_group
      this.user_gender=orderInfo.user_gender

      this.if_first_order=orderInfo.if_first_order

      this.user_id=orderInfo.user_id
    }
  }


  def mergeOrderDetail(orderDetail: OrderDetail): Unit ={
    if(orderDetail!=null){
      this.order_detail_id=orderDetail.id
      this.sku_id=orderDetail.sku_id
      this.sku_name=orderDetail.sku_name
      this.sku_price=orderDetail.order_price
      this.sku_num=orderDetail.sku_num

      this.spu_id =orderDetail.spu_id
      this.tm_id =orderDetail.tm_id
      this.category3_id =orderDetail.category3_id
      this.spu_name =orderDetail.spu_name
      this.tm_name =orderDetail.tm_name
      this.category3_name =orderDetail.category3_name

    }
  }*/
