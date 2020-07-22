package com.lb

object SpuAmountSumApp {
  def main(args: Array[String]): Unit = {

  }

}


/*
 def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ads_spu_amount_sum_app")

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "DWS_ORDER_DETAIL_WIDE";
    val groupId = "ads_spu_amount_sum_group"


    /////////////////////  偏移量处理///////////////////////////
    val offset: Map[TopicPartition, Long] = OffsetManagerM.getOffset(groupId, topic)

    var inputDstream: InputDStream[ConsumerRecord[String, String]] = null
    // 判断如果从redis中读取当前最新偏移量 则用该偏移量加载kafka中的数据  否则直接用kafka读出默认最新的数据
    if (offset != null && offset.size > 0) {
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, offset, groupId)
    } else {
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    //取得偏移量步长
    var offsetRanges: Array[OffsetRange] = null
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = inputDstream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    val orderDstreamDetailWideDstream: DStream[OrderDetailWide] = inputGetOffsetDstream.map { record =>
      val jsonStr: String = record.value()
      val orderDetailWide: OrderDetailWide = JSON.parseObject(jsonStr, classOf[OrderDetailWide])
      orderDetailWide
    }
    val orderWideWithSpuDstream: DStream[(String, Double)] = orderDstreamDetailWideDstream.map(orderWide=>(orderWide.spu_id+":"+orderWide.spu_name,orderWide.final_detail_amount))

    val spuAmountDstream: DStream[(String, Double)] = orderWideWithSpuDstream.reduceByKey(_+_)

    spuAmountDstream.foreachRDD { rdd =>
      val resultArr: Array[(String, Double)] = rdd.collect()
      if (resultArr != null && resultArr.size > 0) {
        DBs.setup()
        DB.localTx(implicit session => {
          val dateTime: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())
          for ((spu, amount) <- resultArr) {
            val spuArr: Array[String] = spu.split(":")
            val spuId: String = spuArr(0)
            val spuName: String = spuArr(1)
            SQL("INSERT INTO spu_order_final_detail_amount_stat(stat_time,spu_id, spu_name, amount) VALUES (?,?,?,?)").bind(dateTime, spuId, spuName, amount).update().apply()
          }
          throw new RuntimeException("测试异常！！")
          for (offset <- offsetRanges) {
            //主键相同替换 主键不同新增
            SQL("replace INTO offset_2020(group_id,topic, partition_id, topic_offset) VALUES (?,?,?,?)").bind(groupId, topic, offset.partition, offset.untilOffset).update().apply()
          }

        }

        )
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
 */