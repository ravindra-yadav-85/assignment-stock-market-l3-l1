package com.ravindra.batch

import java.io.File
import com.ravindra.utility.Common
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}

object BatchApp extends App with Common {

  val parser = new scopt.OptionParser[Arguments]("Parsing application") {

    opt[String]('c', "configFile").
      required().valueName("").action((value, arguments) => arguments.copy(configPath = value))
  }

  def main(arguments: Arguments): Unit = {
    println("config File:" + arguments.configPath)
    //Load the config file
    val config: Config = ConfigFactory.parseFile(new File(arguments.configPath))

    //create a spark session
    val sparkSession: SparkSession = sparkSessions(config.getString("spark_app_name"))

    //read the input file
    val loadCsvInput: DataFrame = readFromCsv(sparkSession,
      config.getString("input_path"),
      config.getString("input_delimiter"),
      config.getString("input_header"))

    //convert Level L3 to Level L1
    val l1Data = getLevelL1RelevantFields(loadCsvInput, sparkSession)

    //calculate the num of partitions
    val numOfPartitions = retunNumOfPartition(l1Data, config.getString("block_size").toInt)

    //save the output file
    writeToCsv(l1Data, config.getString("output_path"),
      config.getString("output_delimiter"),
      config.getString("output_header"),
      numOfPartitions)

    //stop the spark session
    sparkSessionStop(sparkSession)
  }

  parser.parse(args, Arguments()) match {
    case Some(arguments) => main(arguments)
    case None =>
  }

  case class Arguments(configPath: String = "")
}