package com.joseph.sparkhdfs

import com.joseph.util.HdfsUtil
import java.util.logging.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext


object FileEight111 {
  def main(args: Array[String]): Unit = {
    if (null == args || args.length != 2) {
       println("输入的参数个数不对，Uasge file1 file2")
    }
    val sparkConf = new SparkConf().setAppName("FileEight")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    
    val output = "/tmp/hlw/output"
    val hu = new HdfsUtil
    hu.del(output)//删除存放数据的目录
    
    import sqlContext.implicits._
    
    class tatal(id:String)
    class nokey(id:String)
    
    val file1 = args(0)
    val file2 = args(1)
    
    val totalkey = sc.textFile(file1).toDF().as("tatal")
    totalkey.registerTempTable("tatalkey")
    
    val nokey = sc.textFile(file2).toDF().as("nokey")
    nokey.registerTempTable("nokey1")
    
    val sqlstr = "select * from tatalkey a where NOT EXISTS ( select b.id from nokey1 b where b.id = a.id)"
    val yeskey = sqlContext.sql(sqlstr).save(output)
    
    
  }
}