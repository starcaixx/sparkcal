package com.lb.util

import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData, Statement}


object MysqlUtil {

  def main(args: Array[String]): Unit = {
    println("haha")
  }

  def queryList(sql:String): Unit ={
    Class.forName("com.mysql.jdbc.Driver")
    val conn: Connection = DriverManager.getConnection("jdbc:mysql://master:3306/gmall?characterEncoding=utf-8&userSSL=false","root","mima123")
    val stat: Statement = conn.createStatement()
    println(sql)
    val rs: ResultSet = stat.executeQuery(sql)
    val md: ResultSetMetaData = rs.getMetaData
    while (rs.next()) {
      for (i <- 1 to md.getColumnCount){
        println(md.getColumnName(i)+":"+rs.getObject(i))
      }
    }
    stat.close()
    conn.close()
  }

}