package com.joseph.ml

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.joseph.util.HdfsUtil
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating


object movie {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("FileeGs")
    val sc = new SparkContext(sparkConf)

    
    val output = "/tmp/hlw/output/movie.txt"
    val hu = new HdfsUtil
    hu.del(output)//删除存放数据的目录
    
    val rawData = sc.textFile("/tmp/hlw/input/u.data")
    val rawRatings = rawData.map { _.split("\t").take(3) }
    val ratings = rawRatings.map { case Array(user, movie, rating) => Rating(user.toInt, movie.toInt, rating.toDouble) }
    
    val model = ALS.train(ratings, 50, 10, 0.01)
    
    model.userFeatures
    
    val predictedRating = model.predict(789, 123)
    
    val topKRecs = model.recommendProducts(789, 10)
    
    val movies = sc.textFile("/tmp/hlw/input/u.item")
    
    val titles = movies.map( line => line.split("\\|").take(2) ).map ( array => (array(0).toInt, array(1)) ).collectAsMap()
    
    val moviesForUser = ratings.keyBy {_.user }.lookup(789)
    
    val titleAndRate = moviesForUser.sortBy {-_.rating }.take(10).map { rating => (titles(rating.product),rating.rating) }
    titleAndRate.foreach(println)
    
   // hu.writeText(titleAndRate.mkString("\n"), output)
    
    topKRecs.map { rating => (titles(rating.product), rating.rating) }.foreach(println)
    
   // val aMatrix = new DoubleMatrix(Array(1.0, 2.0, 3.0))
    
    
  }
}