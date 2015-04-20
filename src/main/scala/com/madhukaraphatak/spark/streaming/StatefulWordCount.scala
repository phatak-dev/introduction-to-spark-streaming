package com.madhukaraphatak.sparktraining.streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._

object StatefulWordCount {

  /**
   * Maintain wordcount across multiple batches
   * * Takes
   *  args(0) - master Url.
   *  args(1) - hostname of machine that has stream
   *  args(2) - port
   *  args(3) - check point directory
   */
  def main(args: Array[String]) {

    def updateFunction(values: Seq[Int], runningCount: Option[Int]) = {
      val newCount = values.sum + runningCount.getOrElse(0)
      new Some(newCount)
    }
    val ssc = new StreamingContext(args(0), "Statefulwordcount", Seconds(1))
    val lines = ssc.socketTextStream(args(1), args(2).toInt)
    ssc.checkpoint(args(3))
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    val totalWordCount = wordCounts.updateStateByKey(updateFunction _)
    totalWordCount.print()
    ssc.start()
    ssc.awaitTermination()
  }


}
