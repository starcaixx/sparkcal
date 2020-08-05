package com.lb.util

import java.sql.{Connection, DriverManager}
import java.util
import java.util.Properties

import com.mysql.fabric.jdbc.FabricMySQLDriver
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisPool}

object JdbcUtils {
  private val prop = new Properties()
  prop.load(this.getClass.getClassLoader.getResourceAsStream("jdbc.properties"))

//  val driver = prop.getProperty("druid.driverClassName")
//  val jdbcUrl = prop.getProperty("druid.url")
//  val jdbcUser = prop.getProperty("druid.username")
//  val jdbcPassword = prop.getProperty("druid.password")

//  private val dataSources = new util.LinkedList[Connection]()

  private var jedisPool: JedisPool = null

  /*for (i <- 0 to 10) {
    DriverManager.registerDriver(new FabricMySQLDriver)
    //    Class.forName(driver)
    val con = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)
    dataSources.add(con)
  }*/

  /*def getConnection: Connection = {
    dataSources.removeFirst()
  }*/

  def getJedisClient: Jedis = {
    if (jedisPool == null) {
      val poolConfig = new GenericObjectPoolConfig()
      poolConfig.setMaxTotal(prop.getProperty("maxconnects").toInt)
      poolConfig.setMaxIdle(prop.getProperty("maxidle").toInt)
      poolConfig.setMinIdle(prop.getProperty("minidle").toInt)
      poolConfig.setMaxWaitMillis(prop.getProperty("maxwaitmills").toInt)
      poolConfig.setBlockWhenExhausted(true)
      poolConfig.setTestOnBorrow(true)
      jedisPool = new JedisPool(poolConfig,
        prop.getProperty("redisHost"),
        prop.getProperty("redisPort").toInt,
        prop.getProperty("redisTimeout").toInt)
    }

    jedisPool.getResource
  }

  /*def releaseConnection(conn: Connection) = {
    dataSources.add(conn)
  }*/
}
