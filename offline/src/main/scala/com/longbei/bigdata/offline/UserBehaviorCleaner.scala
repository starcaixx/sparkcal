package com.longbei.bigdata.offline

import java.net.URLDecoder

import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object UserBehaviorCleaner {
  def main(args : Array[String]): Unit ={

    if(args.length != 2){
      println("Usage:please input inputPath and outputPath")
      System.exit(1)
    }

    // 获取输入输出路径
    val inputPath = args(0)
    val outputPath = args(1)


    print(inputPath+":out:"+outputPath)
    val conf = new SparkConf().setAppName(getClass.getSimpleName).setMaster("local[4]")
    val sc = new SparkContext(conf)

    val hdfs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)
    if (hdfs.exists(new Path(outputPath))) {
      hdfs.delete(new Path(outputPath),true)
    }

    // 通过输入路径获取RDD
    val eventRDD: RDD[String] = sc.textFile(inputPath)

    // 清洗数据，在算子中不要写大量业务逻辑，应该将逻辑封装到方法中
    eventRDD.filter(event => checkEventValid(event))  // 验证数据有效性
      .map(event=>urlDecode(event))//url解码
      .coalesce(3)
      .saveAsTextFile(outputPath)

    sc.stop()
  }

  /**
    * username为用户自定义的，里面有要能存在"\n"，导致写入到HDFS时换行
    * @param event
    */
  def repairUsername(event : String)={
    val fields = event.split("\t")

    // 取出用户昵称
    val username = fields(1)

    // 用户昵称不为空时替换"\n"
    if(username != "" && !"Null".equals(username)){
      fields(1) = username.replace("\n","")
    }

    fields.mkString("\t")
  }

  /**
    * 脱敏手机号
    * @param event
    */
  def maskPhone(event : String): String ={
    var maskPhone = new StringBuilder
    val fields: Array[String] = event.split("\t")

    // 取出手机号
    val phone = fields(9)

    // 手机号不为空时做掩码处理
    if(phone != null && !"".equals(phone)){
      maskPhone = maskPhone.append(phone.substring(0,3)).append("xxxx").append(phone.substring(7,11))
      fields(9) = maskPhone.toString()
    }
    fields.mkString("\t")
  }

  /**
    * url解码
    * @param event
    * @return
    */
  def urlDecode(event: String): String = {
    val str = URLDecoder.decode(event.split("\t")(9), "utf-8")
    print(str)
    str
  }

  /**
    * 验证数据格式是否正确，只有切分后长度为10的才算正确
    * @param event
    */
  def checkEventValid(event : String) ={
    val fields = event.split("\t")
    fields.length == 10 && !"".equals(fields(9))
  }
}
