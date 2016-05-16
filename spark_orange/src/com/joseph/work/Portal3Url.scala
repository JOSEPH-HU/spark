package com.joseph.work

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.joseph.util.HdfsUtil

object Portal3Url {
  def main(args: Array[String]): Unit = {
    
    if (null == args || args.length != 2) {
      println("输入的参数个数不对，Uasge file1 file2")
    }
    val sparkConf = new SparkConf().setAppName("Portal3Url")
    val sc = new SparkContext(sparkConf)

    val output = "/tmp/hlw/output"
    val hu = new HdfsUtil
    hu.del(output) //删除存放数据的目录

    val file1 = args(0) //材多多文件
    val file2 = args(1) //微门户文件
    
    val cdd = sc.textFile(file1).cache().collect()
    
    val rs = sc.textFile(file2).map { line => line.split("\t").take(10) }.filter { line => "1".equals(line(8)) && getTrue(line(1), cdd) }
    .map { line => 
      "http://www.hc360.com/cp/" + line(2) + ".html"
    }
    rs.saveAsTextFile(output)
    
  
  }
  def getTrue(word:String, Cdd: Array[String]):Boolean ={
    var flag = false
    Cdd.foreach { line => 
     if (word.toLowerCase().contains(line.toLowerCase())) {
       flag = true
     }  
    }
    
    flag
  }
}