package com.joseph.work

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.joseph.util.HdfsUtil

object ChineseToEnglish {
  def main(args: Array[String]): Unit = {
     if (null == args || args.length != 1) {
      println("输入的参数个数不对，Uasge file1")
    }
    val sparkConf = new SparkConf().setAppName("Portal3Url")
    val sc = new SparkContext(sparkConf)

    val output = "/tmp/hlw/output"
    val hu = new HdfsUtil
    hu.del(output) //删除存放数据的目录
  }
}