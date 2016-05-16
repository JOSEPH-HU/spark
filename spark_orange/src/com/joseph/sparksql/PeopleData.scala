package com.joseph.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.log4j.Logger
import java.io.FileWriter
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import java.io.PrintWriter

object PeopleData {
    val log = Logger.getLogger(getClass().getName())
  private val schema = "id,gender,height"
  
  def main(args: Array[String]): Unit = {
      log.error("start--------------")
    val conf = new SparkConf().setAppName("PeopleData")
    val sc = new SparkContext(conf)
    
    val peopleDataRDD = sc.textFile("/tmp/body.data")
    
    val sqlctx = new SQLContext(sc)
    
    val schemArray = schema.split(",")
    val schemas = StructType(schemArray.map { fieldName => StructField(fieldName,StringType,true) })
    
    val rowRDD:RDD[Row] = peopleDataRDD.map(_.split(",")).map { line => Row(line(0), line(1),line(2)) }
    
    val peopleDF = sqlctx.createDataFrame(rowRDD, schemas)
    
    peopleDF.registerTempTable("people")
    
    val highterFemal170 = sqlctx.sql("select id, gender,height from people where height > 170 and gender = 'F'")
    
    println("男生身高为170的总数=" + highterFemal170.count())
    
    log.error("男生身高为170的总数=" + highterFemal170.count())
    
    
    peopleDF.groupBy(peopleDF("gender")).count().map { line => line(0) + "\t" + line(1) }.saveAsTextFile("/tmp/hlw")
    
    
    
  /*  println("=================")
    
    peopleDF.filter(peopleDF("gender").equalTo("M")).filter(peopleDF("height") > 210).show(20)
    
    
    println("=================")
    
    peopleDF.sort($"height".desc).take(50).foreach{row => println(row(0) + "," + row(1) + "," + row(2))}
    
    peopleDF.where($"height" === 120)*/
    
    sc.stop()
    
  }
    
    /**
   * 写入到文件
   * line:内容
   * pathStr:路径
   */
  def writeText(line: String, pathStr: String) {
    val fs = FileSystem.get(new Configuration())
    val writer = new PrintWriter(fs.create(new Path(pathStr)))
    writer.println(line)
    writer.close()
  }
}