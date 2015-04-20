package com.madhukaraphatak.sparktraining.streaming.windowoperation

import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * Send text lines over socket and do a batch wise word count with sliding window
 *
 * Takes
 *  args(0) - master Url.
 *  args(1) - hostname of machine that has stream
 *  args(2) - port
 *
 */
object WindowWordCount {
  def main(args: Array[String]) {
    val ssc = new StreamingContext(args(0), "window wordcount", Seconds(10))
    val lines = ssc.socketTextStream(args(1),args(2).toInt)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKeyAndWindow((a:Int,b:Int) => (a + b),Seconds(30),Seconds(10))
    wordCounts.print()
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }

}
