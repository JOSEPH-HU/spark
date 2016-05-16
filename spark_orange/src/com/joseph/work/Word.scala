package com.joseph.work

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.joseph.util.HdfsUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

object Word {
  case class CSort(cword: String, ccode: String)
  case class VSort(vword: String, vcode: String, status: String,bcnum:Int,pin:String)
  def main(args: Array[String]): Unit = {
    if (null == args || args.length != 2) {
      println("输入的参数个数不对，Uasge file1 file2")
    }
    val sparkConf = new SparkConf().setAppName("Word")
    val sc = new SparkContext(sparkConf)

    val sqlCtx = new SQLContext(sc)
    import sqlCtx.implicits._

    val output = "/tmp/hlw/output"
    val hu = new HdfsUtil
    hu.del(output) //删除存放数据的目录

    val file1 = args(0) //标准文件
    val file2 = args(1) //微门户全部数据

    val s1 = sc.textFile(file1).map(_.split("\t")).map { case Array(l1:String, l2:String) => CSort(l1, l2) }.toDF()
    s1.registerTempTable("csort")
    s1.cache()

    val s2 = sc.textFile(file2).map(_.split("\\|").take(5)).filter { case Array(l1:String,l2:String, l3:String, l4:String, l5:String) => l3.equals("1") && l1.length > 2 && l1.length < 6  }.map { case Array(l1:String,l2:String, l3:String, l4:String, l5:String) => VSort(l1, l2, l3,l4.toInt, l5) }.toDF()
   // val s2 = sc.textFile(file2).map(_.split("|")).map { case Array(l1:String,l2:String, l3:String) => VSort(l1, l2, l3) }.toDF()
    s2.registerTempTable("vsort")
    s2.persist(StorageLevel.MEMORY_AND_DISK)

    val tt = sqlCtx.sql("select c.cword,c.ccode,v.vword,v.pin from csort c,vsort v where c.ccode = v.vcode").map { row => row(0) + "," + row(1) + "," + row(2) + "," + row(3)}.saveAsTextFile(output)
    
   // tt.save(output)

  }
}