package com.longbei.bigdata.offline

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object ParsLog {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("usage:xxx input output")
      System.exit(1)
    }
    val input = args(0)
    val output = args(1)

    val sparkConf: SparkConf = new SparkConf().setAppName(getClass.getSimpleName)
    val sc = new SparkContext(sparkConf)

    val session: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    import session.implicits._

    val watchvideoRDD = session.sparkContext.textFile(input)


    session.stop()
    /*val activesRDD: RDD[ActiveInfo] = activeRDD.filter(_.split("\t").length == 17).mapPartitions(actives => {
      actives.map(active => {
        val infos: Array[String] = active.split("\t")
        ActiveInfo(infos(0), infos(1), infos(2), infos(3).toInt, infos(4).toInt, infos(5), infos(6), infos(7), infos(8),
          infos(9).replaceAll("(\\d{3})\\d{4}(\\d{4})", "$1****$2"),
          infos(10), infos(11).toInt, infos(12).toInt, infos(13).toLong, infos(14), infos(15), infos(16).toLong)
      })
    })
    val resultRDD: RDD[(String, ActiveInfo)] = activesRDD.groupBy(active => {
      active.uid + ":" + active.eventKey + ":" + active.eventTime
    }).mapValues(actives => {
      actives.toList.take(1)
    }).values.flatMap(ls => ls).map(active => {
      val localDateTime: LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(active.eventTime), ZoneId.systemDefault())
      import java.time.format.DateTimeFormatter
      val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
      (localDateTime.format(formatter), active)
    }).partitionBy(new HashPartitioner(7))

    resultRDD.saveAsHadoopFile(output,classOf[String],classOf[String],classOf[PairRDDMultipleTextOutputFormat])
*/

    //        println(resultRDD.count())
    //        resultRDD.saveAsTextFile(output)
    //        resultRDD.toDF().write.format("text").save(output)
  }

}
