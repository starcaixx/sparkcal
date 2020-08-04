package com.lb.util

import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData, Statement}

import com.alibaba.fastjson.JSONObject

import scala.collection.mutable.ListBuffer

object PhoenixUtil {
  def main(args: Array[String]): Unit = {
    println(queryList("select state,city,population from US_POPULATION"))
  }

  def queryList(sql: String): List[JSONObject] = {
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    val connection: Connection = DriverManager.getConnection("jdbc:phoenix:master:2181")
    val stat: Statement = connection.createStatement()
//    println("sql:"+sql)
    val rs: ResultSet = stat.executeQuery(sql)
    val md: ResultSetMetaData = rs.getMetaData
    val resultList = new ListBuffer[JSONObject]()
    while (rs.next()) {
      val rowData = new JSONObject()
      for (i <- 1 to md.getColumnCount) {
        rowData.put(md.getColumnName(i), rs.getObject(i))
      }
      resultList += rowData
    }
    stat.close()
    connection.close()
    resultList.toList
  }
}
