package com.joseph.sparkhdfs

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.joseph.util.HdfsUtil

object FileeGs {
  def main(args: Array[String]): Unit = {
     if (null == args || args.length != 1) {
       println("输入的参数个数不对，Uasge file1")
    }
    val sparkConf = new SparkConf().setAppName("FileeGs")
    val sc = new SparkContext(sparkConf)

    
    val output = "/tmp/hlw/output"
    val hu = new HdfsUtil
    hu.del(output)//删除存放数据的目录
    

    val file1 = args(0)
    
    val testfiles = sc.textFile(file1).map { line => pp(line) }.saveAsTextFile(output)
  }
  def pp(a1:String):String={
    var hh:StringBuffer = new StringBuffer(" ")
    if (null != a1 && !a1.equals("")) {
      hh.append("db.s_top_bc_boutique.remove({'key_id':").append(a1).append(",'list_date':'201512'});").append("\n")
      hh.append("db.s_top_bc_new.remove({'key_id':").append(a1).append(",'list_date':'201512'});").append("\n")
      hh.append("db.s_top_bc_popu.remove({'key_id':").append(a1).append(",'list_date':'201512'});").append("\n")
      hh.append("db.s_top_bc_potential.remove({'key_id':").append(a1).append(",'list_date':'201512'});").append("\n")
      hh.append("db.s_top_keycompany_active.remove({'key_id':").append(a1).append(",'list_date':'201512'});").append("\n")
      hh.append("db.s_top_keycompany_credit.remove({'key_id':").append(a1).append(",'list_date':'201512'});").append("\n")
      hh.append("db.s_top_keycompany_new.remove({'key_id':").append(a1).append(",'list_date':'201512'});").append("\n")
      hh.append("db.s_top_keycompany_popu.remove({'key_id':").append(a1).append(",'list_date':'201512'});").append("\n")
    }
    hh.toString()
  }
}