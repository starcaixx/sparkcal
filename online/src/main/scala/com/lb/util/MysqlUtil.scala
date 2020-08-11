package com.lb.util

import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData, Statement}
import java.util.Properties
import java.util
import com.mysql.fabric.jdbc.FabricMySQLDriver


object MysqlUtil {

  def main(args: Array[String]): Unit = {
    println("haha")
  }

  def queryList(sql: String): Unit = {
//    Class.forName("com.mysql.jdbc.Driver")
//    val conn: Connection = DriverManager.getConnection("jdbc:mysql://master:3306/gmall?characterEncoding=utf-8&userSSL=false", "root", "mima123")
    val conn: Connection = getConnection
    val stat: Statement = conn.createStatement()
    println(sql)
    val rs: ResultSet = stat.executeQuery(sql)
    val md: ResultSetMetaData = rs.getMetaData
    while (rs.next()) {
      for (i <- 1 to md.getColumnCount) {
        println(md.getColumnName(i) + ":" + rs.getObject(i))
      }
    }
    stat.close()
    conn.close()
  }


  private val prop = new Properties()
  prop.load(this.getClass.getClassLoader.getResourceAsStream("jdbc.properties"))
  val driver = prop.getProperty("druid.driverClassName")
  val jdbcUrl = prop.getProperty("druid.url")
  val jdbcUser = prop.getProperty("druid.username")
  val jdbcPassword = prop.getProperty("druid.password")

  private val dataSources = new util.LinkedList[Connection]()

  for (i <- 0 to 10) {
    DriverManager.registerDriver(new FabricMySQLDriver)
    //    Class.forName(driver)
    val con = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)
    dataSources.add(con)
  }

  def getConnection: Connection = {
    dataSources.removeFirst()
  }

}
