package com.spark.smacd

import java.util.ArrayList

import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters

object SmacdDriver {

  /**
    * Main method intended to be called from `spark-submit`.
    * @param args
    */
  def main(args: Array[String]): Unit = {
    import org.apache.spark.SparkConf
    val sparkConf = new SparkConf().setAppName("SMACD APP").setMaster("local[2]").set("spark.executor.memory", "2g")
    val sc = new SparkContext(sparkConf)

    val sparkSession = SparkSession.builder().appName("SMACD Example").getOrCreate()
    val stockData: DataFrame = readDataFrame(sparkSession, "data/sp1985.csv")
    val divisorTable: DataFrame = calculateDivisorTable(stockData)

    val stats: ArrayList[Stat] = calculateStats(divisorTable)
    printStats(stats)

    val statsDF: DataFrame = statsArrayListToDF(stats, sparkSession)
    statsDF.show()
    statsDF.write.mode(SaveMode.Overwrite).parquet("./target/stats.parquet")
  }

  /**
   * Maps an rdd row to csv row
   * @param stat
   * @return
   */
  def toCSVLine(stat: Stat): String = {
    val result: String = stat.positionPrice+","+stat.lastPosition+","+stat.winCount+","+stat.lossCount+","+stat.tieCount+","+stat.grandTotal

    return result
  }

  /**
   * Helper function to convert arraylist of Stat objects to a DataFrame of stat objects
   * @param stats
   */
  def statsArrayListToDF(stats: ArrayList[Stat], sparkSession: SparkSession): DataFrame = {
    val resSeq: Seq[Stat] = JavaConverters.collectionAsScalaIterableConverter(stats).asScala.toSeq
    val statsDF: DataFrame = sparkSession.createDataFrame(resSeq)
    return statsDF
  }

  /**
   * Helper function to convert arraylist of Stat objects to an RDD of stat objects
   * @param stats
   */
  def statsArrayListToRDD(stats: ArrayList[Stat], sparkSession: SparkSession): RDD[Stat] = {
    val resSeq: Seq[Stat] = JavaConverters.collectionAsScalaIterableConverter(stats).asScala.toSeq
    val statsRDD: RDD[Stat] = sparkSession.createDataFrame(resSeq).rdd.flatMap[Stat](rowToStat)
    return statsRDD
  }

  /**
   * Calculates the results of the given systemTable. An unlimited number of systems can be calculated here.
   * @param systemTable
   * @return
   */
  def calculateStats(systemTable: DataFrame): ArrayList[Stat] = {
    val results : ArrayList[Stat] = new ArrayList[Stat]()

    val cols: Array[String] = systemTable.columns
    if(cols.length > 2) {
      var i: Int = 0
      for (i <- 2 to (cols.length-1)) {
        val colName: String = cols(i)
        val dataPoints: RDD[DataPoint] = convertToRddOfDataPoints(systemTable.select("increment", "price", cols(i)))
        val stat: Stat = calculateStat(dataPoints)
        results.add(stat)
      }
    }
    return results
  }

  /**
   * Prints one Stat object
   * @param stat
   */
  def printStat(stat: Stat) = {
    printf("winCount: %d, lossCount: %d, tieCount: %d, grandTotal: %f \n", stat.winCount, stat.lossCount, stat.tieCount, stat.grandTotal)
  }

  /**
   * Prints all Stat objects in the given ArrayList
   * @param stats
   */
  def printStats(stats: ArrayList[Stat]) = {
    var i = 0
    for(i <- 0 to (stats.size()-1)){
      printStat(stats.get(i))
    }
  }

  /**
   * Reads data from given path.
   * @param sparkSession
   * @param path
   * @return
   */
  def readDataFrame(sparkSession: SparkSession, path: String): DataFrame = {
    val csvReadOptions = Map("inferSchema" -> true.toString, "header" -> true.toString)
    val data = sparkSession.read.options(csvReadOptions).csv(path)
    data
  }

  /**
   * Converts a calculationTable DataFrame to an RDD of DataPoints
   * @param df a DataFrame that adheres to the following interface: 3 columns: increment, price, divVal
   */
  def convertToRddOfDataPoints(df: DataFrame): RDD[DataPoint] ={
    val dataPoints: RDD[DataPoint] = df.rdd.flatMap(rowToDataPoint)
    dataPoints
  }

  /**
   * Converts a row from an RDD to a DataPoint
   * @param row
   * @return
   */
  def rowToDataPoint(row: Row): Option[DataPoint] = {
    row match {
      case Row(increment: Int, price: Double, divVal: Double) => Some(DataPoint(increment, price, divVal))
      case _ => None
    }
  }

  /**
   * Converts a row from an RDD to a Stat
   * @param row
   * @return
   */
  def rowToStat(row: Row): Option[Stat] = {
    row match {
      case Row(positionPrice: Double, lastPosition: Int, winCount: Int, lossCount: Int, tieCount: Int, grandTotal: Double) => Some(Stat(positionPrice, lastPosition, winCount, lossCount, tieCount, grandTotal))
      case _ => None
    }
  }

  /**
   * Calculates the stats on an RDD of DataPoints: grand total, win count, loss count, etc
   * @param dataPoints
   * @return
   */
  def calculateStat(dataPoints: RDD[DataPoint]): Stat = {
    val baseVal = new Stat(0, 0,0, 0, 0, 0)

    //the sequencing/looping over each dataPoint. Where stats is the accumulation.
    val seqOp = (agg: Stat, dataPoint: DataPoint) => {
      var winLoss: Double = 0
      var lastPosition: Int = agg.lastPosition
      var positionNow: Int  = 0
      var winCount: Int = agg.winCount
      var lossCount: Int = agg.lossCount
      var tieCount: Int = agg.tieCount
      var grandTotal: Double = agg.grandTotal

      if(dataPoint.divVal < 0 ){ //short the market
        positionNow = -1
      }else if(dataPoint.divVal >= 0){ //long the market
        positionNow = 1
      }
      var positionPrice: Double = agg.positionPrice

      if(positionNow != agg.lastPosition){ //a position change aka a trade occurs
        positionPrice = dataPoint.price
        if(lastPosition == -1){
          winLoss = agg.positionPrice - dataPoint.price
        }else if(lastPosition == 1){
          winLoss = dataPoint.price - agg.positionPrice
        }
        if(winLoss < 0){
          lossCount += 1
        }else if(winLoss > 0){
          winCount += 1
        }else{
          tieCount += 1
        }
        grandTotal += winLoss
      }

      val newCalc = new Stat(positionPrice, positionNow, winCount, lossCount, tieCount, grandTotal)
      newCalc
    } //takes a item and a seq, returns a seq

    val comboOp = (a: Stat, b: Stat) => {
      val newStats = new Stat(0,0, a.winCount + b.winCount, a.lossCount + b.lossCount, a.tieCount + b.tieCount, a.grandTotal + b.grandTotal)
      newStats
    } //puts two seqs together
    dataPoints.aggregate(baseVal)(seqOp, comboOp)
  }

  /**
   * Adds several divisor columns to the stockData DataFrame. One or more divisors are used to make systems.
   * @param stockData
   * @return
   */
  def calculateDivisorTable(stockData: DataFrame): DataFrame = {
    val splitMA2 = new SplitMA
    val splitMA4 = new SplitMA
    val splitMA6 = new SplitMA
    val splitMA8 = new SplitMA
    val splitMA10 = new SplitMA
    val splitMA12 = new SplitMA
    val splitMA14 = new SplitMA
    val splitMA16 = new SplitMA
    val splitMA18 = new SplitMA
    val splitMA20 = new SplitMA

    val newDF = stockData
        .withColumn("2d", splitMA2(col("price"), lit(2)).over(Window.orderBy("increment").rowsBetween(-1, 0)))
        .withColumn("4d", splitMA4(col("price"), lit(4)).over(Window.orderBy("increment").rowsBetween(-3, 0)))
        .withColumn("6d", splitMA6(col("price"), lit(6)).over(Window.orderBy("increment").rowsBetween(-5, 0)))
        .withColumn("8d", splitMA8(col("price"), lit(8)).over(Window.orderBy("increment").rowsBetween(-7, 0)))
        .withColumn("10d", splitMA10(col("price"), lit(10)).over(Window.orderBy("increment").rowsBetween(-9, 0)))
        .withColumn("12d", splitMA12(col("price"), lit(12)).over(Window.orderBy("increment").rowsBetween(-11, 0)))
        .withColumn("14d", splitMA14(col("price"), lit(14)).over(Window.orderBy("increment").rowsBetween(-13, 0)))
        .withColumn("16d", splitMA16(col("price"), lit(16)).over(Window.orderBy("increment").rowsBetween(-15, 0)))
        .withColumn("18d", splitMA18(col("price"), lit(18)).over(Window.orderBy("increment").rowsBetween(-17, 0)))
        .withColumn("20d", splitMA20(col("price"), lit(20)).over(Window.orderBy("increment").rowsBetween(-19, 0)))

    newDF.orderBy("increment").show()
    return newDF
  }
}
