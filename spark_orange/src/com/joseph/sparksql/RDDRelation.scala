package com.joseph.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row

object RDDRelation {
  case class Record(key: String, value: String)
  
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("RDDRelation")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    
    import sqlContext.implicits._
    
    val rddFromText = sc.textFile("/tmp/hlw/input/kv.txt").map { line => 
      var ar = line.split("\t") 
      Record(ar(0),ar(1)) }.toDF()
    
    rddFromText.registerTempTable("records")
    
    println("result select:")
   // sqlContext.sql("select key,value from records").map { case Row(key:String, value:String) => s"$key" + "," + s"$value"}.foreach { println}
    sqlContext.sql("select key,value from records").map { row => "key:" + s"${row(0)}" + "," + "value:" + s"${row(1)}" }.foreach(println) 
  }
}