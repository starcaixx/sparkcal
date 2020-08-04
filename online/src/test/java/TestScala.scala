import java.time.LocalDateTime

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object TestScala {
  def main(args: Array[String]): Unit = {
    val str: String = List("abcd","efg","haha","hehe","a").mkString("','")
    println(str)
  }

}
