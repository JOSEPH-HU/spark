package com.joseph.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import com.joseph.util.HdfsUtil

object CreateDataFrame {
  def main(args: Array[String]): Unit = {
    val output = "/tmp/hlw/output"
    val hu = new HdfsUtil
    hu.del(output)//删除存放数据的目录
    
    val conf = new SparkConf().setAppName("CreateDataFrame")
    val sc = new SparkContext(conf)
    
    
    val sqlContext = new SQLContext(sc)
    
    val df = sqlContext.read.json("/tmp/hlw/input/people.json") //把文件装换成dataframe 格式 age name 
    
    
    //df.map { line => line(0) + "\t" + line(1) }.saveAsTextFile("/tmp/hlw/output")
    
    //|--age
    //|--name 转换成一颗树
    df.printSchema() 
    
    //df.select("name").map { line => line(0) }.saveAsTextFile(output)//输出名字
    
    //df.select(df("name"), df("age") + 1).map { line => line(0) + "\t" + line(1) }.saveAsTextFile(output)
    
  //  df.filter(df("age")>21).map { line => line(0) + "\t" + line(1) }.saveAsTextFile(output)
    
   df.groupBy("age").count().map { line => line(0) + "\t" + line(1) }.saveAsTextFile(output)
    
    
    
    
  }
}