package com.joseph.sparksql

import com.joseph.util.HdfsUtil
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object DataSource {
  def main(args: Array[String]): Unit = {
    val output = "/tmp/hlw/output"
    val hu = new HdfsUtil
    hu.del(output) //删除存放数据的目录

    val conf = new SparkConf().setAppName("DataSource")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    
    val df = sqlContext.read.load("/tmp/hlw/input/users.parquet")
    
    df.select("name", "favorite_colo").write.save("/tmp/hlw/output/namesAndFavColors.parquet")
    sc.stop()
  }
}