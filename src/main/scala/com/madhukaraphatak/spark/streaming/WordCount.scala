package com.madhukaraphatak.sparktraining.streaming

import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * Send text lines over socket and do a batch wise word count
 *
 * Takes
 *  args(0) - master Url.
 *  args(1) - hostname of machine that has stream
 *  args(2) - port
 *
 */
object WordCount {
  def main(args: Array[String]) {
    val ssc = new StreamingContext(args(0), "wordcount", Seconds(20))
    val lines = ssc.socketTextStream(args(1),args(2).toInt)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }

}
