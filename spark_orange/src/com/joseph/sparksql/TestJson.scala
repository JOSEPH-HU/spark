package com.joseph.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object TestJson {
  
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("TestJson")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    
    import sqlContext.implicits._
    
    case class bc(_id:String, key_id:Int, bc_id:String, bc_trend:Int,sort:Int,bc_state:Int,list_date:String)
    case class keyword(_id:String, key_id:Int, keyword:String,state:String)
    val path = "/tmp/hlw/input/test.json"
    val bcjson = sqlContext.read.json(path).as("bc")
    bcjson.registerTempTable("abc")
    
   /* val teenagers = sqlContext.sql("select bc_id from abc").cache()
    teenagers.foreach {println }*/
    println("======================")
    
    val pathkey = "/tmp/hlw/input/key1.json"
    val bcjsonkey = sqlContext.read.json(pathkey).as("keyword")
    bcjsonkey.registerTempTable("keyword")
    
    val teenagersq = sqlContext.sql("select * from keyword, abc where abc.bc_id = keyword.key_id")
    println("===================")
    teenagersq.collect().foreach {println}
     println("===================")
  }
}