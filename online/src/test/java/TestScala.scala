import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object TestScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[3]")
    val sc = new SparkContext(conf) //初始化StreamingContext，并设置batch时间片间隔为15秒
    val ssc = new StreamingContext(sc,Seconds(3))
    //从socket的9999端口获取数据
    val data = ssc.socketTextStream("localhost", 9999)
    data.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()
    //启动任务
    ssc.start() //等待任务中断停止
    ssc.awaitTermination()
  }

}
