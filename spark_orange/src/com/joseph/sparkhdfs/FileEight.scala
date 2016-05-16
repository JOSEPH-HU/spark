package com.joseph.sparkhdfs

import com.joseph.util.HdfsUtil
import java.util.logging.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.dmg.pmml.True


object FileEight {
  def main(args: Array[String]): Unit = {
    if (null == args || args.length != 2) {
       println("输入的参数个数不对，Uasge file1 file2")
    }
    val sparkConf = new SparkConf().setAppName("FileEight")
    val sc = new SparkContext(sparkConf)

    
    val output = "/tmp/hlw/output"
    val hu = new HdfsUtil
    hu.del(output)//删除存放数据的目录
    

    val file1 = args(0)
    val file2 = args(1)
    
     val nokey = sc.textFile(file2).cache()
    
    val totalkey = sc.textFile(file1)
    
    totalkey.subtract(nokey).saveAsTextFile(output)

  }
}