import java.time.{LocalDate, LocalDateTime}

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object TestScala {
  def main(args: Array[String]): Unit = {
    val str: String = List("abcd","efg","haha","hehe","a").mkString("','")
    println(str)

    val i: Int = "2020-08-04 17:38:26".compareTo("2020-08-04 17:38:27")
    println(i)

    val jsonStr = "{\"database\":\"gmall\",\"xid\":469272,\"data\":{},\"commit\":true,\"type\":\"insert\",\"table\":\"base_province\",\"ts\":1596612026}"
    val jsonStrNotEmp = "{\"database\":\"gmall\",\"table\":\"base_province\",\"type\":\"bootstrap-insert\",\"ts\":1596598446,\"data\":{\"id\":1,\"name\":\"北京\",\"region_id\":\"1\",\"area_code\":\"110000\",\"iso_code\":\"CN-11\"}}"
    val obj: JSONObject = JSON.parseObject(jsonStr)
    val objNotEmp: JSONObject = JSON.parseObject(jsonStrNotEmp)
    val dataObjNotEmp: JSONObject = objNotEmp.getJSONObject("data")
    val dataObj: JSONObject = obj.getJSONObject("data")
    println(dataObj.keySet().size())
    println(dataObjNotEmp)
    println(dataObj)
    println(dataObjNotEmp.isEmpty)
    println(dataObj.isEmpty)
    println(dataObjNotEmp.size())
    println(dataObj.size())

    println("==============")
    val date: LocalDate = LocalDate.now()
    println(date)
  }

}
