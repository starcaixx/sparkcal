package com.lb.sparktest.dim

import java.time.{LocalDate, Period}
import java.time.format.DateTimeFormatter
import java.util.ResourceBundle

import com.alibaba.fastjson.{JSON, JSONObject}
import com.lb.sparktest.bean.UserInfo
import com.lb.util.MyKafkaConsumer
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UserInfoApp {
  private val bundle: ResourceBundle = ResourceBundle.getBundle("jdbc")

  def main(args: Array[String]): Unit = {
//    val checkpoint = ""
    val checkpoint = "/Users/longbei/IdeaProjects/sparkcal/sparktest/userinfo"
    val ssc: StreamingContext = StreamingContext.getOrCreate(checkpoint,()=>userInfoSync(checkpoint))

    ssc.start()
    ssc.awaitTermination()
  }

  //todo 维度表合并在一个程序中完成
  def userInfoSync(checkpoint: String): StreamingContext = {
    val topic: String = "ODS_T_USER_INFO"
    val dbIndex: Int = bundle.getString("localDbIndex").toInt
    val conf: SparkConf = new SparkConf().setAppName(getClass.getSimpleName)
      .setMaster("local[3]")
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
    val transFormDS: DStream[(String, String)] = kafkaDS.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })
    val userInfoDS: DStream[UserInfo] = transFormDS.map(record => {
      //json字符串有与样例类同字段时才会进行赋值成功
      val userInfo: UserInfo = JSON.parseObject(record._2, classOf[UserInfo])

      val dtf: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
      val localDate: LocalDate = LocalDate.parse(userInfo.birthday, dtf)
      val period: Period = Period.between(localDate, LocalDate.now)
      val age = period.getYears
      if (age < 20) {
        userInfo.age_group = "20-"
      } else if (age > 30) {
        userInfo.age_group = "30+"
      }
      if (userInfo.gender == "M") {
        userInfo.gender_name = "man"
      } else {
        userInfo.gender_name = "female"
      }
      userInfo
    })
    userInfoDS.foreachRDD(rdd=>{
      import org.apache.phoenix.spark._
      rdd.saveToPhoenix("GMALL_USER_INFO",Seq("ID","USER_LEVEL","BIRTHDAY","GENDER","AGE_GROUP","GENDER_NAME"),new Configuration(),Some("node:2181"))
      MyKafkaConsumer.saveOffsetToRedis(dbIndex,offsetRanges)
    })

    ssc
  }

}
