package com.lb.util

import java.sql.DriverManager
import java.util.Properties

object JdbcUtils {
  private val prop = new Properties()
  prop.load(this.getClass.getClassLoader.getResourceAsStream("jdbc.properties"))

}
