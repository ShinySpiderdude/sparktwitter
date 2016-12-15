package com.tikalk.sparktwitter

import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by ilan on 12/15/16.
  */
object WordCount {


  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "WordCount")
    val textFile = sc.textFile("/tmp/words")



    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey((currentTotal, currentValue) => currentTotal + currentValue)
    val cacheCounts = counts.cache()
    cacheCounts.collect().foreach(println)
    cacheCounts.map(result => result._1.toUpperCase()).collect().foreach(println)
  }
}
