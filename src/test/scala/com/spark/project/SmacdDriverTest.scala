package com.spark.project

import com.spark.smacd.SmacdDriver
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext, SparkSession}
import org.scalatest.FunSuite

class SmacdDriverTest extends FunSuite {
  val spark = SparkSession.builder().appName("SMACD App").master("local[*]").getOrCreate()
  import spark.implicits._  //This import is done on the spark session instantiated above. required for serializing DataPoint objects.

  test("SMACD test1: read and show statsDF"){
    val statsDF = spark.read.parquet("./target/stats.parquet")
    statsDF.show()
  }
}
