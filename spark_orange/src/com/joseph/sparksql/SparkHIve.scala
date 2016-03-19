package com.joseph.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext

object SparkHIve {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkHIve")
    val sc = new SparkContext(sparkConf)
    
    val hiveContext = new HiveContext(sc)
    hiveContext.setConf("database", "test")
    
    import hiveContext.implicits._
    import hiveContext.sql
    
    
//    println("Result of 'SELECT *': ")
//    sql("SELECT * FROM test.src").collect().foreach(println)
    
    
//    val count = sql("select count(*) from test.src ").collect().head.getLong(0)
//    println(s"COUNT(*):$count")
//    
//    val rddFromSql = sql("select key, value from src where key <10 order by key")
//    println("result:")
//    val rddAsStrins = rddFromSql.map { case Row(key:Int, value:String) => s"Key: $key, Value:$value" }.foreach(println)
    
    
      val rddFromSql = sql("select sen_source,originalcurrenturl,pv,uv,clicknum,irsl_date from test.ctr");
    
      val rddAsStr = rddFromSql.map { case Row(sen_source:String, originalcurrenturl:String, pv:Int, uv:Int, clicknum:Int,irsl_date:String) 
        => url(s"$originalcurrenturl") + "," + "hlw" + "," + s"$pv"}
      rddAsStr.saveAsTextFile("/tmp/hlw/out/hive")
      
      
      sc.stop()
  }
  
  def url(url:String):String= {
    println("hhhhhhhhhhh=" + url) 
    var flag = "0"
    if (url.contains("http://b2b.hc360.com/")) {
      flag = "1"
    }else {
      flag = "2"
    }
    flag
  }
}