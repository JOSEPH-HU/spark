package com.joseph.sparksql

import com.joseph.util.HdfsUtil
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD

object DataSets {
  def main(args: Array[String]): Unit = {
    val output = "/tmp/hlw/output"
    val hu = new HdfsUtil
    hu.del(output) //删除存放数据的目录

    val conf = new SparkConf().setAppName("CreateDataFrame")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._ //不导入这个 toDF()会报错
    case class Person(var name: String, age: Int)

    //val people = sc.textFile("/tmp/input/people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt)).toDF() //有错误

   // people.registerTempTable("people")

  }
}