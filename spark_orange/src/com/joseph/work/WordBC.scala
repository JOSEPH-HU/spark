package com.joseph.work

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.joseph.util.HdfsUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

object WordBC {
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


    val s2 = sc.textFile(file2).map(_.split("\\|")).filter { case Array(l1:String,l2:String, l3:String, l4:String,l5:String) => l5.equals("1") && l4.toInt == 0 }
    s2.cache()
    
    val zero = s2.count()
    
   hu.writeText("商机数为零的关键词" + zero, output + "/zero/zero.txt")
    
   val rr = s2.map { case Array(l1:String,l2:String, l3:String, l4:String,l5:String)  => l1 + "," + l4}
    rr.saveAsTextFile(output + "/zerotxt")
    
    val s3 = sc.textFile(file2).map(_.split("\\|")).filter { case Array(l1:String,l2:String, l3:String, l4:String, l5:String) => l5.equals("1") && l4.toInt >0 && l4.toInt < 50}
    s3.cache()
    
    val five = s3.count()
    
    hu.writeText("商机数大于零小于50的关键词" + five, output + "/zerofive/zerofive.txt")
    
    val gg = s3.map { case Array(l1:String,l2:String, l3:String, l4:String, l5:String)  => l1 + "," + l4}
    gg.saveAsTextFile(output + "/zerofivetxt")
  }
}