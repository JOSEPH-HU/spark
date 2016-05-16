package com.joseph.sparkgraph

import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

object PgaeRank {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PgaeRank")
    val sc = new SparkContext(conf)
    
    val graph = GraphLoader.edgeListFile(sc, "/tmp/hlw/input/followers.txt")
    
    val ranks =graph.pageRank(0.0001).vertices
    
    ranks.foreach(println)
    
    println("===================")
    
    val users = sc.textFile("/tmp/hlw/input/users.txt").map{line => 
      val fields = line.split(",")
      (fields(0).toLong,fields(1))
      }
    
    val ranksByUsername = users.join(ranks).map{case (id, (username, rank)) => (username,rank)}
    
    println(ranksByUsername.collect().mkString("\n"))
  }
}