package com.ravindra.streaming

import java.io.File

import com.ravindra.utility.Common
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode}

object StreamingApp extends App with Common {

  case class Arguments(configPath: String = "")

  val parser = new scopt.OptionParser[Arguments]("Parsing application") {

    opt[String]('c', "configFile").
      required().valueName("").action((value, arguments) => arguments.copy(configPath = value))
  }

  def main(arguments: Arguments): Unit = {

    println("config File:" + arguments.configPath)
    //Load the config file
    val config: Config = ConfigFactory.parseFile(new File(arguments.configPath))

    // Create Spark Session
    val sparkSession = sparkSessions(config.getString("spark_app_name"))

    //Define the schema for the Input
    val schema = StructType(List(
      StructField("seq_num", LongType, true),
      StructField("add_order_id", LongType, true),
      StructField("add_side", StringType, true),
      StructField("add_price", DoubleType, true),
      StructField("add_qty", IntegerType, true),
      StructField("update_order_id", LongType, true),
      StructField("update_side", DoubleType, true),
      StructField("update_price", DoubleType, true),
      StructField("update_qty", IntegerType, true),
      StructField("delete_order_id", LongType, true),
      StructField("delete_side", DoubleType, true),
      StructField("trade_order_id", LongType, true),
      StructField("trade_side", DoubleType, true),
      StructField("trade_price", DoubleType, true),
      StructField("trade_qty", IntegerType, true),
      StructField("time", TimestampType, true)
    ))

    // Create Streaming DataFrame by reading data from File Source.
    val initDF = readFileStream(sparkSession, config.getString("input_file_format"),
      config.getString("input_header"),
      config.getString("max_files_per_trigger").toInt,
      config.getString("input_stream_dir"),
      schema)

    // Check if DataFrame is streaming or Not.
    println("Is this Streaming DataFrame : " + initDF.isStreaming)

    // Print Schema of DataFrame
    println("Schema of DataFame initDF ...")
    println(initDF.printSchema())

    //Run the aggregation
    val resultDF = initDF.withWatermark("time", "30 minutes")

    //write it to output and start streaming for new files
    resultDF
      .writeStream
      .outputMode(config.getString("output_mode"))
      .foreachBatch(writeToFile)
      .option("startingOffset", config.getString("clean_source"))
      .option("forceDeleteTempCheckpointLocation", true)
      .option("spark.sql.streaming.fileSource.log.cleanupDelay", config.getString("cleanup_delay").toInt)
      .option("spark.sql.streaming.fileSource.log.compactInterval", config.getString("compact_interval").toInt)
      .option("cleanSource", config.getString("clean_source"))
      .option("sourceArchiveDir", config.getString("source_archive_dir"))
      .option("checkpointLocation", config.getString("checkpoint_dir"))
      .start()
      .awaitTermination()

    //stop the sparkSession
    sparkSession.stop()

    //Store the output
    def writeToFile = (df: DataFrame, batchId: Long) => {
      //convert L3 to L1
      val dfL3ToL1 = getLevelL1RelevantFields(df, sparkSession)
      //calculate the num of partitions
      val numOfPartitions = retunNumOfPartition(dfL3ToL1, config.getString("block_size").toInt)

      dfL3ToL1
        .repartition(numOfPartitions)
        .write.option("delimiter", config.getString("output_delimiter"))
        .option("header", config.getString("output_header"))
        .mode(SaveMode.Overwrite)
        .csv(config.getString("output_path"))
    }
  }

  parser.parse(args, Arguments()) match {
    case Some(arguments) => main(arguments)
    case None =>
  }
}