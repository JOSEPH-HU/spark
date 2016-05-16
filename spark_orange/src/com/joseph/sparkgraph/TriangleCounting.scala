package com.joseph.sparkgraph

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx.PartitionStrategy

object TriangleCounting {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TriangleCounting")
    val sc = new SparkContext(conf)
    
    
    val graph = GraphLoader.edgeListFile(sc, "/tmp/hlw/input/followers.txt",true).partitionBy(PartitionStrategy.RandomVertexCut)
    
    val tricounts = graph.triangleCount().vertices
    
    println("========================================================")
    println(tricounts.collect().mkString("\n"))
    println("========================================================")
    
    val users = sc.textFile("/tmp/hlw/input/users.txt").map { line => 
       val fields = line.split(",")
       (fields(0).toLong, fields(1))
       }
    
    val tricountByUsername = users.join(tricounts).map{
      case(id, (username,tc)) => (username, tc)
    }
    
    println("========================================================")
    println(tricountByUsername.collect().mkString("\n"))
    println("========================================================")
  }
}