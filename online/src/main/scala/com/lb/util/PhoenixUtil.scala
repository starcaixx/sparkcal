package com.lb.util

import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData, Statement}

object PhoenixUtil {
  def main(args: Array[String]): Unit = {
    queryList("select * from USER_STATE2020")
  }

  def queryList(sql:String): Unit ={
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    val connection: Connection = DriverManager.getConnection("jdbc:phoenix:master:2181")
    val stat: Statement = connection.createStatement()
    println(sql)
    val rs: ResultSet = stat.executeQuery(sql)
    val md: ResultSetMetaData = rs.getMetaData
    while (rs.next()) {
      for (i<- 1 to md.getColumnCount){
        println(md.getColumnName(i)+":"+rs.getObject(i))
      }
    }
    stat.close()
    connection.close()
  }
  /*
  def   queryList(sql:String):List[JSONObject]={
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    val resultList: ListBuffer[JSONObject] = new  ListBuffer[ JSONObject]()
    val conn: Connection = DriverManager.getConnection("jdbc:phoenix:hadoop1,hadoop2,hadoop3:2181")
    val stat: Statement = conn.createStatement
    println(sql)
    val rs: ResultSet = stat.executeQuery(sql )
    val md: ResultSetMetaData = rs.getMetaData
    while (  rs.next ) {
      val rowData = new JSONObject();
      for (i  <-1 to md.getColumnCount  ) {
        rowData.put(md.getColumnName(i), rs.getObject(i))
      }
      resultList+=rowData
    }

    stat.close()
    conn.close()
    resultList.toList
  }*/

}
