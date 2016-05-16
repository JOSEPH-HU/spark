package com.joseph.Stream

import com.joseph.util.HdfsUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel

object SocketStream {
  def main(args: Array[String]): Unit = {
    
    if (null == args || args.length != 2) {
      println("Uage: ip port")
      System.exit(1)
    }
    
    val output = "/tmp/hlw/output/socket"
    val hu = new HdfsUtil
    hu.del(output)//删除存放数据的目录
    
    val conf = new SparkConf().setAppName("SocketStream")
    val ssc = new StreamingContext(conf,Seconds(1))
    
    val lines = ssc.socketTextStream(args(0), args(1).toInt)
    
    val words = lines.flatMap (_.split(" "))
    val pairs = words.map { word => (word,1) }
    val wordCounts = pairs.reduceByKey(_ + _)
    
    //wordCounts.foreachRDD(lines=>lines.saveAsTextFile(output))
    
    wordCounts.print()
    
    ssc.start()
    ssc.awaitTermination()
  }
}