package com.ravindra.utility

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.util.SizeEstimator

import scala.collection.mutable.Map

trait Common {

  //time,bid_price,ask_price,bid_size,ask_size,seq_num
  case class LevelL1RelevantFields(time: String, bid_price: Double, ask_price: Double, bid_size: Int, ask_size: Int, seq_num: Long)


  /**
   * Creates a SparkSession
   * * @param appName
   */
  def sparkSessions(appName: String): SparkSession = {
    val session = SparkSession
      .builder()
      .master("local")
      .appName(appName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    session
  }
  /**
   * stop sparksession
   * @param sparkSession
   */
  def sparkSessionStop(sparkSession: SparkSession): Unit = {
    sparkSession.stop();
  }

  /**
   * reading table format input
   * @param sparkSession
   * @param inputPath
   * @param inputDelimiter
   */
  def readFromCsv(sparkSession: SparkSession, inputPath: String, inputDelimiter: String, inputHeader: String): DataFrame ={
    return sparkSession.read.option("delimiter", inputDelimiter).option("header", inputHeader).csv(inputPath)
  }

  /**
   * reading table format input
   * @param dataFrame
   * @param outputPath
   * @param outputDelimiter
   */

  def writeToCsv(dataFrame: DataFrame, outputPath: String, outputDelimiter: String, outputHeader: String, numOfPartitions: Int): Unit ={
    dataFrame.repartition(numOfPartitions).write.option("delimiter", outputDelimiter).option("header", outputHeader).mode(SaveMode.Overwrite).csv(outputPath)
  }

  def writeToCsv(dataFrame: DataFrame, batchId: Long, outputPath: String, outputDelimiter: String, outputHeader: String, numOfPartitions: Int): Unit ={
    dataFrame.repartition(numOfPartitions).write.option("delimiter", outputDelimiter).option("header", outputHeader).mode(SaveMode.Overwrite).csv(outputPath)
  }

  /**
   * returns the size of rdd
   * @param rdd
   */

  def calcRDDSize(rdd: RDD[String]): Long = {
    rdd.map(_.getBytes("UTF-8").length.toLong)
      .reduce(_+_) //add the sizes together
  }

  /**
   * return num of partitions
   * @param df
   * @param blockSize
   */
  def retunNumOfPartition(df: DataFrame, blockSize: Int): Int = {
    val rddOfDataframe = df.rdd.map(_.toString())
    val sizes = SizeEstimator.estimate(rddOfDataframe)
    var numOfPartitions = ((sizes/1048576)/blockSize).toInt
    if (numOfPartitions == 0) numOfPartitions=1

    numOfPartitions
  }

  /**
   * return the file stream dataframe
   * @param sparkSession
   * @param inputFormat
   * @param header
   * @param maxFilesPerTrigger
   * @param streamPath
   * @param schemas
   */
  def readFileStream(sparkSession: SparkSession, inputFormat: String, header: String, maxFilesPerTrigger: Int,
                     streamPath: String, schemas: StructType): DataFrame ={
     val dataFrame = sparkSession
      .readStream
      .format(inputFormat)
      .option("maxFilesPerTrigger", maxFilesPerTrigger) // This will read maximum of 2 files per mini batch. However, it can read less than 2 files.
      .option("header", header)
      .option("path", streamPath)
      .schema(schemas)
      .load()

    dataFrame
  }

  def getLevelL1RelevantFields(inputLevell3RDD: DataFrame, sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    var bestBidAsk = Map("bid_price" -> 0.0, "bid_size" -> 0.0, "ask_price" -> 0.0, "ask_size" -> 0.0)
    var bid_price = 0.0
    var bid_size = 0

    var ask_price = 0.0
    var ask_size = 0
    //time,bid_price,ask_price,bid_size,ask_size,seq_num
    inputLevell3RDD.map(rec => {
      //val col = rec.split(",")
      //seq_num,add_order_id,add_side,add_price,add_qty,update_order_id,update_side,update_price,update_qty,delete_order_id,delete_side,trade_order_id,trade_side,trade_price,trade_qty,time
      val time = rec(15).asInstanceOf[String]
      //add order id
      if (rec(1) != null) {
        if (rec(2) == "BUY") {
          bid_price = rec(3).asInstanceOf[Double]
          bid_size = rec(4).asInstanceOf[Int]

          ask_price = (bestBidAsk.get("ask_price")).asInstanceOf[Double]
          ask_size = (bestBidAsk.get("ask_size")).asInstanceOf[Int]

          if (bid_price < (bestBidAsk.get("bid_price")).asInstanceOf[Double]) {
            bestBidAsk.put("bid_price", bid_price)
          }

        } else {
          ask_price = rec(3).asInstanceOf[Double]
          ask_size = rec(4).asInstanceOf[Int]

          bid_price = (bestBidAsk.get("bid_price")).asInstanceOf[Double]
          bid_size = (bestBidAsk.get("bid_size")).asInstanceOf[Int]

          if (ask_price > (bestBidAsk.get("ask_price")).asInstanceOf[Double]) {
            bestBidAsk.put("ask_price", ask_price)
          }
        }
      }
      //update order id
      else if (rec(5) != null)
        if (rec(6) == "BUY") {
          bid_price = rec(7).asInstanceOf[Double]
          bid_size = rec(8).asInstanceOf[Int]

          ask_price = (bestBidAsk.get("ask_price")).asInstanceOf[Double]
          ask_size = (bestBidAsk.get("ask_size")).asInstanceOf[Int]

          if (bid_price < (bestBidAsk.get("bid_price")).asInstanceOf[Double]) {
            bestBidAsk.put("bid_price", bid_price)
          }

        } else {
          ask_price = rec(7).asInstanceOf[Double]
          ask_size = rec(8).asInstanceOf[Int]

          bid_price = (bestBidAsk.get("bid_price")).asInstanceOf[Double]
          bid_size = (bestBidAsk.get("bid_size")).asInstanceOf[Int]

          if (ask_price > (bestBidAsk.get("ask_price")).asInstanceOf[Double]) {
            bestBidAsk.put("ask_price", ask_price)
          }
        }

      //delete order id
      else if (rec(9) != null) {

      }
      //trade order id
      else if (rec(11) != null) {
      }

      val seq_num = rec(0).asInstanceOf[Long]

      //time,bid_price,ask_price,bid_size,ask_size,seq_num
      LevelL1RelevantFields(time, bid_price, ask_price, bid_size, ask_size, seq_num)
    }).toDF()
  }
}
