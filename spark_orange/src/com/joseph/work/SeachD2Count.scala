package com.joseph.work

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.joseph.util.HdfsUtil
import org.apache.hadoop.fs.Path
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONObject
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.tools.GetConf

object SeachD2Count {
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
    val paths = hu.subdirs2(inpath, "2016-04-29-09")

    var set2: Set[Path] = Set()

    while (paths.hasNext) {
      var tt = paths.next().getParent()
      set2 += tt
    }

    var results = set2.foreach { path =>
      var filenames = path.toString().split("\\/")
println("==============" + path + "==" + filenames.length)
      var ip = filenames(6)
      var modelnum = filenames(7)
      var count = 0//总次数
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
            }
            
          }
          ("D2" + "\t" + ip + "\t" + modelnum + "\t" + datetime,value)
        }
        .groupByKey
        //result1.saveAsTextFile(output)
        
        result1.map{t =>
          t._2.foreach { line =>  
             count  = count + 1
              totalTime = totalTime + line
              if (line <= 100) {
                count1 = count1 + 1
              }
              if(line >100 && line <=300){
                count3 = count3 + 1
              }
              if(line>300 && line <=500){
                count5 = count5 + 1
              }
              if(line > 500 && line <= 1000){
                count10 = count10 + 1
              }
              if (line > 1000) {
                count30 = count30 + 1
              }
          
          }
          t._1 + "\t" + count + "\t" + totalTime + "\t" + count1 + "\t" + count3 + "\t" + count5 + "\t" + count10 + "\t" + count30
        }
        
   }
    

  }

  def handleJson(jsonstr: String): Int = {
    
    var wait = 0
    var ti = 0
    try {
      val jsonobject:JSONObject = JSON.parseObject(jsonstr.trim())
//println(jsonstr.trim())
      val tt = jsonobject.getJSONObject("da")
      ti = tt.getIntValue("ti")
      
      val ww = tt.getString("in")
      
      val rr = ww.split(",")
      
     val waits = rr(10).split(":")
     
     wait = waits(1).toInt
      
      
    } catch {
      case t: Throwable =>  // TODO: handle error
    }
   // println("wait=" + wait + ";ti=" + ti)
    wait + ti
  }
}