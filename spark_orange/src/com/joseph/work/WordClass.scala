package com.joseph.work

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.joseph.util.HdfsUtil

object WordClass {
  def main(args: Array[String]): Unit = {
     if (null == args || args.length != 1) {
      println("输入的参数个数不对，Uasge file1")
    }
    val sparkConf = new SparkConf().setAppName("FileEight")
    val sc = new SparkContext(sparkConf)


    val output = "/tmp/hlw/output"
    val hu = new HdfsUtil
    hu.del(output) //删除存放数据的目录

    val file2 = args(0) //微门户全部数据
    
    val s2 = sc.textFile(file2).map { line => line.split("\t").take(6) }.filter { words => words(5).equals("044") }
    .sortBy(words=>words(4)+words(3)).map { words =>  
      words(5) + "," + words(4) + "," + words(3) + "," + words(1)
    }.saveAsTextFile(output)
  }
}