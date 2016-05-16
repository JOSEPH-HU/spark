package com.joseph.work

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.joseph.util.HdfsUtil
import org.apache.hadoop.fs.Path
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONObject

object SeachD2CountBack {
  def main(args: Array[String]): Unit = {
    if (null == args || args.length != 2) {
      println("输入的参数个数不对，Uasge input output ")
    }
    val sparkConf = new SparkConf().setAppName("SeachD2Count")
    val sc = new SparkContext(sparkConf)

    // val output = "/tmp/hlw/output"
    val output = args(1)
    val hu = new HdfsUtil
    hu.del(output) //删除存放数据的目录

    val input = args(0) //是输入文件目录
    val inpath = new Path(input)
    val paths = hu.subdirs2(inpath, "2016-05-03-09")
    //paths.foreach{uu => println("===========" + uu)}

    var set2: Set[Path] = Set()

    while (paths.hasNext) {
      var tt = paths.next().getParent()
      set2 += tt
    }
set2.foreach{uu => println("=====" + uu.toString())}
    var results = set2.foreach { path =>

      val filenames = path.toString().split("\\/")

      val ip = filenames(6)
      val modelnum = filenames(7)
      println("ip=" + ip + ";modelnum=" + modelnum)
      var count:Int = 0//总次数
			var totalTime = 0//总时间
			var count1=0//0~100ms的次数
			var count3=0//100~300ms的次数
			var count5 =0//300~500ms的次数
			var count10 =0//500~1000ms的次数
			var count30 =0//1000ms以上的次数
			var value = 0;
      var datetime = ""

      val result1 = sc.textFile(path.toString()).filter { line => line.contains("wait") && line.contains("ti") }
        .map { line =>
          val jsonTmp = line.split("\\ -\\ ")
          datetime = jsonTmp(0).substring(0, 13)
          if (null != jsonTmp && jsonTmp.length > 1) {
            val jsonTmp1 = jsonTmp(1).split("-\\ ")
            if (jsonTmp1 != null && jsonTmp1.length > 1) {
              value = handleJson(jsonTmp1(0))
              count  = count + 1
              totalTime = totalTime + value
              if (value <= 100) {
                count1 = count1 + 1
              }
              if(value >100 && value <=300){
                count3 = count3 + 1
              }
              if(value>300 && value <=500){
                count5 = count5 + 1
              }
              if(value > 500 && value <= 1000){
                count10 = count10 + 1
              }
              if (value > 1000) {
                count30 = count30 + 1
              }
            }
          }
          "" + "\t" + "D2" + "\t" + ip + "\t" + modelnum + "\t" + datetime + "\t" + count + "\t" + totalTime + "\t" + count1 + "\t" 
          + count3 + "\t" + count5 + "\t" + count10 + "\t" + count30
        }
    }
    

  }

  def handleJson(jsonstr: String): Int = {
    var wait = 0
    var ti = 0
    try {
      val jsonobject:JSONObject = JSON.parseObject(jsonstr)

      val ww = jsonobject.getIntValue("wait")
      val tt = jsonobject.getIntValue("ti")
      ww + tt
    } catch {
      case t: Throwable =>  // TODO: handle error
    }
    wait + ti
  }
}