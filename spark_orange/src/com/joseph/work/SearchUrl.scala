package com.joseph.work

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import com.joseph.util.HdfsUtil
import org.apache.spark.rdd.RDD

object SearchUrl {
  def main(args: Array[String]): Unit = {
    if (null == args || args.length != 3) {
      println("输入的参数个数不对，Uasge file1 file2 flag(1-热搜 2-s域)")
    }
    val sparkConf = new SparkConf().setAppName("SearchUrl")
    val sc = new SparkContext(sparkConf)

    val output = "/tmp/hlw/output"
    val hu = new HdfsUtil
    hu.del(output) //删除存放数据的目录

    val file1 = args(0) //材多多文件
    val file2 = args(1) //热搜文件
    val flag = args(2)
    
    val cdd = sc.textFile(file1).cache().collect()
    
    val rs = sc.textFile(file2).map { line => line.split("\t").take(10) }.filter { line => "1".equals(line(2)) && getTrue(line(1), cdd) }
    .map { line => 
      val id = line(0)
      val keyword = line(1)
      val dir = line(9)
    if (null != id && !"".equals(id)) {
      if(null != dir && !"".equals(dir)){
        if ("1".equals(flag)) {
          "http://www.hc360.com/hots-" + dir + "/" + id + ".html"
        }else if("2".equals(flag)){
          "http://s.hc360.com/?w=" + keyword + "&mc=seller"
        }
      }else{
         if ("1".equals(flag)) {
          "http://www.hc360.com/hots/" + id + ".html"
        }else if("2".equals(flag)){
          "http://s.hc360.com/?w=" + keyword + "&mc=seller"
        }
      }
    }
    }
    rs.saveAsTextFile(output)
    
   // tt.save(output)
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