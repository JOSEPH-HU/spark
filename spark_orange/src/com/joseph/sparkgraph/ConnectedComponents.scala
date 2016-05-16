package com.joseph.sparkgraph

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.GraphLoader

object ConnectedComponents {
  def main(args: Array[String]): Unit = {
     val conf = new SparkConf().setAppName("ConnectedComponents")
    val sc = new SparkContext(conf)
     
     val graph = GraphLoader.edgeListFile(sc, "/tmp/hlw/input/followers.txt")
     val cc = graph.connectedComponents().vertices
      println("===========================================================")
      println(cc.collect().mkString("\n"))
      println("===========================================================")
     
     val users = sc.textFile("/tmp/hlw/input/users.txt").map { line => 
       val fields = line.split(",")
       (fields(0).toLong, fields(1))
       }
     val ccByusername = users.join(cc).map{
       case (id, (username, cc)) => (username, cc)
     }
     
     println("===========================================================")
     println(ccByusername.collect().mkString("\n"))
     println("===========================================================")
  }
}