package com.madhukaraphatak.sparktraining.streaming

import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * * Takes
 *  args(0) - master Url.
 *  args(1) - hostname of machine that has stream
 *  args(2) - port
 *  args(3) - check point directory
 *  args(4) - path to customer master data
 */
object CartCustomerJoin {

  def main(args: Array[String]) {

    def updateFunction(rows:Seq[Double], runningValue:Option[Double]) = {
      val newValue = rows.sum + runningValue.getOrElse(0.0)
      Some(newValue)
    }

    val ssc = new StreamingContext(args(0), "NetworkWordCount", Seconds(10))


    val networkStream = ssc.socketTextStream(args(1),args(2).toInt)

    ssc.checkpoint(args(3))

    val customerDataRDD = ssc.sparkContext.textFile(args(4)).map( row =>{
      val columnValues = row.split(",")
      (columnValues(0),columnValues(1))
    })

    /**
     * The input data is a comma separated with following columns
     *
     * transactionId,customerId,itemId,itemValue
     */

    val cartStream = networkStream.map(row => {
      val columnVales = row.split(",")
      val customerId = columnVales(1)
      (customerId,row)
    })


    val joinRDD = cartStream.transform(cartRDD => {
      customerDataRDD.join(cartRDD).map {
        case (customerId,(customerName,sales)) => {
          (customerName,sales)
        }
      }
    })


    val perCustomerSalesStream = joinRDD.map{
      case(customerName,salesRecord) => {
        val salesAmount = salesRecord.split(",")(3).toDouble
        (customerName,salesAmount)
      }
    }

    val perCustomerSales = perCustomerSalesStream.updateStateByKey(updateFunction _)

    perCustomerSales.print()

    ssc.start()
    ssc.awaitTermination()


  }


}
