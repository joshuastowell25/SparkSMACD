package com.spark.project

import com.spark.smacd.{SmacdDriver, Stat}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import scala.concurrent.duration._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers

class SmacdDriverTest extends FunSuite  with Matchers{
  val spark = SparkSession.builder().appName("SMACD App").master("local[*]").getOrCreate()
  import spark.implicits._  //This import is done on the spark session instantiated above. required for serializing DataPoint objects.

  test("SMACD test1: read and show statsDF"){
    val statsDF = spark.read.parquet("./target/stats.parquet")
    statsDF.show()
  }

  test("max win/loss ratio"){
    val statsDF = spark.read.parquet("./target/stats.parquet")
    val result: Any= SmacdDriver.projectQuestion1(statsDF)
    print("result: "+result)
    result must equal(1.8181818181818181)
  }

  test("max grand total"){
    val statsDF = spark.read.parquet("./target/stats.parquet")
    val result: Any= SmacdDriver.projectQuestion2(statsDF)
    result must equal(3239.8140029999995)
  }

  test("count grandTotal > 0"){
    val statsDF = spark.read.parquet("./target/stats.parquet")
    val result: Any= SmacdDriver.projectQuestion3(statsDF)
    print("result: "+result)
    result must equal(49)
  }

  test("count grandTotal < 0"){
    val statsDF = spark.read.parquet("./target/stats.parquet")
    val result: Any= SmacdDriver.projectQuestion4(statsDF)
    print("result: "+result)
    result must equal(95)
  }

  test("count grandTotal == 0"){
    val statsDF = spark.read.parquet("./target/stats.parquet")
    val result: Any= SmacdDriver.projectQuestion5(statsDF)
    print("result: "+result)
    result must equal(0)
  }

  test("sum of all grand totals"){
    val statsDF = spark.read.parquet("./target/stats.parquet")
    val result: Any= SmacdDriver.projectQuestion6(statsDF)
    print("result: "+result)
    result must equal(-109494.24104399994)
  }

  test("avg grand total of systems containing the number 20"){
    val statsDF = spark.read.parquet("./target/stats.parquet")
    val result: Any= SmacdDriver.projectQuestion7(statsDF)
    print("result: "+result)
    result must equal(-858.3263809062503)
  }

  test("avg grand total of all systems"){
    val statsDF = spark.read.parquet("./target/stats.parquet")
    val result: Any= SmacdDriver.projectQuestion8(statsDF)
    print("result: "+result)
    result must equal(-760.3766739166663)
  }

  test("avg grand total of systems not containing the number 20"){
    val statsDF = spark.read.parquet("./target/stats.parquet")
    val result: Any= SmacdDriver.projectQuestion9(statsDF)
    print("result: "+result)
    result must equal(-732.3910433482132)
  }
}
