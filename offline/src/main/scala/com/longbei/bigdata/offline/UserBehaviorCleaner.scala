package com.longbei.bigdata.offline

import java.net.URLDecoder

import com.longbei.bigdata.comm.util.ConfigurationUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
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

    initKerberos()
//    val conf = new SparkConf().setAppName(getClass.getSimpleName).setMaster("local[4]")
//    val sc = new SparkContext(conf)
    val session: SparkSession = SparkSession.builder().appName(getClass.getSimpleName).master("local[4]")
  .enableHiveSupport().getOrCreate()

    import session.implicits._
    val sc = session.sparkContext
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)
//    if (hdfs.exists(new Path(outputPath))) {
//      hdfs.delete(new Path(outputPath),true)
//    }

    // 通过输入路径获取RDD
    val eventRDD: RDD[String] = sc.textFile(inputPath)
    // 清洗数据，在算子中不要写大量业务逻辑，应该将逻辑封装到方法中
    val logDS = eventRDD.filter(event => checkEventValid(event)) // 验证数据有效性
      .map(event => urlDecode(event)) //url解码
      .map(event => formatStr(event))
      .toDS()
    logDS


    println(logDS.count())


//      .coalesce(1)
//      .saveAsTextFile(outputPath)

//    session.sql()
    session.stop()
//    sc.stop()
  }

  def initKerberos(): Unit ={
    //kerberos权限认证
//    val path = ConfigurationUtil.getClass.getClassLoader.getResource("").getPath
    val path = "D:/work/gitlab/sparkcal/comm/src/main/resources/"
//    print("path:"+path)
    val principal = ConfigurationUtil.getValueByKey("kerberos.principal")
    val keytab = ConfigurationUtil.getValueByKey("kerberos.keytab")

    val conf = new Configuration
    System.setProperty("java.security.krb5.conf", "C:/ProgramData/MIT/Kerberos5/krb5.ini")
//    conf.addResource(new Path(path + "hbase-site.xml"))
    conf.addResource(new Path(path + "hdfs-site.xml"))
    conf.addResource(new Path(path + "hive-site.xml"))
    conf.set("hadoop.security.authentication", "Kerberos")
    conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
    UserGroupInformation.setConfiguration(conf)
    UserGroupInformation.loginUserFromKeytab(principal, path+keytab)
    println("login user: "+UserGroupInformation.getLoginUser())
  }

  case class LogInfo(enType:String,logStr:String)

  /**
    * 格式化日志为type  log格式
    * @param event
    * @return
    */
  def formatStr(event: String):LogInfo={
    val strPros = event.split("&")
    var eventType=""
    var logStr=""
    strPros.foreach(str=>{
      val props = str.split("=")
      if ("ltype".equals(props(0))) {
        eventType=props(1)
      }else if ("log".equals(props(0))) {
        logStr=props(1)
      }
    })
    LogInfo(eventType,logStr)
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
