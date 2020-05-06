package com.spark.project

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object ExampleDriver {

  /**
    * Main method intended to be called from `spark-submit`.
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SMACD APP").setMaster("local[2]").set("spark.executor.memory", "2g")
    val sc = new SparkContext(sparkConf)

    val distributedSparkSession = SparkSession.builder().appName("SMACD Example").getOrCreate()
    val data = readData(distributedSparkSession, "data/201508_trip_data.csv")
    val result = doubleTripDuration(distributedSparkSession, data)
    result.write.mode(SaveMode.Overwrite).parquet("/target/testing-example-data")
  }

  /**
   * Reads data from given path.
   * @param sparkSession
   * @param path
   * @return
   */
  def readData(sparkSession: SparkSession, path: String): DataFrame = {
    val csvReadOptions =
      Map("inferSchema" -> true.toString, "header" -> true.toString)

    val stationData =
      sparkSession.read.options(csvReadOptions).csv(path)

    stationData
  }

  /**
   * Doubles the trip count of all trips.
   * @param sparkSession
   * @param data
   * @return
   */
  def doubleTripDuration(sparkSession: SparkSession, data: DataFrame): DataFrame = {
    data.select(
      col("end_terminal"),
      col("start_date"),
      col("subscriber_type"),
      col("start_terminal"),
      col("end_station"),
      col("trip_id"),
      expr("duration * 2") as "duration",
      col("bike_number"),
      col("end_date"),
      col("start_station"),
      col("zip_code")
    )
  }

  /**
    * Aggregates duration for all trips.
    * @param sparkSession
    * @param data
    * @return
    */
  def aggregateDuration(sparkSession: SparkSession, data: DataFrame): Long = {
    data.agg(sum("duration")).first.get(0).asInstanceOf[Long]
  }
}
