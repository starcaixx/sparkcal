package com.longbei.bigdata.offline

import java.util.ResourceBundle

import com.alibaba.fastjson.{JSON, JSONObject}
import jodd.util.URLDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object UserBehavior {
  val bundle: ResourceBundle = ResourceBundle.getBundle("config")
  def main(args: Array[String]): Unit = {
//	inputPath:/user/hive/warehouse/ods.db/origin/origin_user_behavior/${day}
//	outputPath:/user/hive/warehouse/tmp.db/use_behavior_${day}

	if (args.length!=2) {
	  println("Usage:please input inputPath and outputPath")
	  System.exit(1)
	}

	val inputPath = args(0)
	val outputPath = args(1)

	val conf: SparkConf = new SparkConf().setAppName(getClass.getSimpleName).setMaster("local[2]")
	val sc = new SparkContext(conf)

	val eventRDD: RDD[String] = sc.textFile(inputPath)

	eventRDD.filter(event=>checkEventVaild(event))
//  	  .map(event => maskPhone(event))
  	  .map(event=>repairUserName(event))
  	  .coalesce(3)
  	  .saveAsTextFile(outputPath)

	sc.stop()
  }

  def checkEventVaild(event: String): Boolean = {
	val fields = event.split("\t")
	fields.length == 10 && !"".equals(fields(9))
  }

  /**
	* 脱敏手机号
	* @param event
	* @return
	*/
  def maskPhone(event: String): String = {
	var maskPhone = new StringBuilder
	val fields: Array[String] = event.split("\t")

	val phone: String = fields(9)
	if (phone !=null && !"".equals(phone)) {
	  maskPhone = maskPhone.append(phone.substring(0,3)).append("xxxx").append(phone.substring(7,11))
	  fields(9)=maskPhone.toString()
	}
	fields.mkString("\t")
  }

  /**
	* username为用户自定义的，里面有要能存在"\n",导致写入到HDFS时换行
	* @param event
	* @return
	*/
  def repairUserName(event: String) = {
	val strs: String = event.split("\t")(9)
	val decode = URLDecoder.decode(strs.replaceAll("%(?![0-9a-fA-F{2}])",""), "utf-8")
	println("decode:"+decode.substring(decode.indexOf("{")))
	val jsonObject: JSONObject = JSON.parseObject(decode.substring(decode.indexOf("{")))

	val fieldsParams: String = bundle.getString("fields.params")
	val fields: Array[String] = fieldsParams.split("-")
	val commFields: Array[String] = fields(0).split(",")
	val eventFields: Array[String] = fields(1).split(",")

	var strTmp = new StringBuilder

	for (elem <- commFields) {
	  strTmp.append(jsonObject.getString(elem)).append("\t")
	}

	for (elem <- eventFields) {
	  strTmp.append(jsonObject.getJSONObject("value").getString(elem)).append("\t")
	}
	strTmp.toString()
  }

}
