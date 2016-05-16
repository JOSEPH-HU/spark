package com.joseph.sparkgraph

import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

object GraphTest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("GraphTest")
    val sc = new SparkContext(conf)

    val users: RDD[(VertexId, (String, String))] = sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
      (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))

    val relationships: RDD[Edge[String]] = sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
      Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))

    val defaultUser = ("John Doe", "Missing")

    val graph = Graph(users, relationships, defaultUser)

    val facts: RDD[String] = graph.triplets.map(triplet => triplet.srcAttr._1 + " is the " + triplet.attr
      + " of " + triplet.dstAttr._1)
    println("=======================")
    facts.collect.foreach(println(_))
    println("=======================")
    val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")

    validGraph.vertices.collect().foreach(println(_))
    println("=======================")
    
    validGraph.triplets.map(triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1).collect().foreach {println}

  }

}