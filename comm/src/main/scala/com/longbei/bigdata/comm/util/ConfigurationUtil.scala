package com.longbei.bigdata.comm.util

import java.util.ResourceBundle

import com.alibaba.fastjson.{JSON, JSONObject}

object ConfigurationUtil {

  // FileBasedConfigurationBuilder:产生一个传入的类的实例对象
  // FileBasedConfiguration:融合FileBased与Configuration的接口
  // PropertiesConfiguration:从一个或者多个文件读取配置的标准配置加载器
  // configure():通过params实例初始化配置生成器
  // 向FileBasedConfigurationBuilder()中传入一个标准配置加载器类，生成一个加载器类的实例对象，然后通过params参数对其初始化
  private val bundle: ResourceBundle = ResourceBundle.getBundle("config")

  def getValueByKeyFromConfig(config:String,key:String):String = {
    ResourceBundle.getBundle(config).getString(key)
  }

  def getValueByKey(key:String):String={
    //    Thread.currentThread().getContextClassLoader.getResourceAsStream("config.properties")
    bundle.getString(key)
  }

  def main(args: Array[String]): Unit = {
    println(getValueByKeyFromConfig("condition","condition.params.json"))
  }

  def getCondValue(key:String): String ={
    val str: String = ResourceBundle.getBundle("condition").getString("condition.params.json")
    val json: JSONObject = JSON.parseObject(str)
    json.getString(key)
  }

}
