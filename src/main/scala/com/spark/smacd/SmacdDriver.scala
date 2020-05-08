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
    var divisorTable: DataFrame = calculateDivisorTable(stockData)

    divisorTable = addSysCol(Array(2), Array(4), divisorTable, sparkSession) //Add the system 2 vs 4
    divisorTable = addSysCol(Array(2,4), Array(), divisorTable, sparkSession) //Add the system 2 vs 4
    divisorTable = addSysCol(Array(2,4,6), Array(), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(2,4,6,8), Array(), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(2,4,6,8,10), Array(), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(2,4,6,8,10,12), Array(), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(2,4,6,8,10,12,14), Array(), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(2,4,6,8,10,12,14,16), Array(), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(2,4,6,8,10,12,14,16,18), Array(), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(2,4,6,8,10,12,14,16,18,20), Array(), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(2,4,6,8,10,12,14,16,18), Array(20), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(2,4,6,8,10,12,14,16), Array(18,20), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(2,4,6,8,10,12,14), Array(16,18,20), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(2,4,6,8,10,12), Array(14,16,18,20), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(2,4,6,8,10), Array(12,14,16,18,20), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(2,4,6,8), Array(10,12,14,16,18,20), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(2,4,6), Array(8,10,12,14,16,18,20), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(2,4), Array(6,8,10,12,14,16,18,20), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(2), Array(4,6,8,10,12,14,16,18,20), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(), Array(), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(), Array(), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(), Array(), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(), Array(), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(), Array(), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(), Array(), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(2,4,6,8,10,12,12), Array(), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(2,4,6,8,10,12,12,12), Array(), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(2,4,6,8,10,12,12,12,12), Array(), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(2,4,6,8,10,12,12,12,12,12), Array(), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(12,12,12,12,12,12), Array(20), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(12,12,12,12,12), Array(20), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(12,12,12,12), Array(20), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(12,12,12), Array(20), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(12,12), Array(20), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(12), Array(20), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(12,12,12,12,12,12), Array(18), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(12,12,12,12,12), Array(18), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(12,12,12,12), Array(18), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(12,12,12), Array(18), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(12,12), Array(18), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(12), Array(18), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(2), Array(20), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(2,4), Array(18,20), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(2,4,6), Array(16,18,20), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(2,4,6,8), Array(14,16,18,20), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(2,4,6,8,10), Array(12,14,16,18,20), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(2,4,6), Array(), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(4,6,8), Array(), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(6,8,10), Array(), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(8,10,12), Array(), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(10,12,14), Array(), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(12,14,16), Array(), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(14,16,18), Array(), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(16,18,20), Array(), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(2,2,2,4,4,10,12), Array(18), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(2,2,2,4,4,10,12), Array(20), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(2,2,2,4,4,10,12), Array(18,20), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(2), Array(4), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(2), Array(6), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(2), Array(8), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(2), Array(10), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(2), Array(12), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(2), Array(14), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(2), Array(16), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(2), Array(18), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(2), Array(20), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(4), Array(2), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(4), Array(6), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(4), Array(8), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(4), Array(10), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(4), Array(12), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(4), Array(14), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(4), Array(16), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(4), Array(18), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(4), Array(20), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(6), Array(2), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(6), Array(4), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(6), Array(8), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(6), Array(10), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(6), Array(12), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(6), Array(14), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(6), Array(16), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(6), Array(18), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(6), Array(20), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(8), Array(2), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(8), Array(4), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(8), Array(6), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(8), Array(10), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(8), Array(12), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(8), Array(14), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(8), Array(16), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(8), Array(18), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(8), Array(20), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(10), Array(2), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(10), Array(4), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(10), Array(6), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(10), Array(8), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(10), Array(12), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(10), Array(14), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(10), Array(16), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(10), Array(18), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(10), Array(20), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(12), Array(2), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(12), Array(4), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(12), Array(6), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(12), Array(8), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(12), Array(10), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(12), Array(14), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(12), Array(16), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(12), Array(18), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(12), Array(20), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(14), Array(2), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(14), Array(4), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(14), Array(6), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(14), Array(8), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(14), Array(10), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(14), Array(12), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(14), Array(16), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(14), Array(18), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(14), Array(20), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(16), Array(2), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(16), Array(4), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(16), Array(6), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(16), Array(8), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(16), Array(10), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(16), Array(12), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(16), Array(14), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(16), Array(18), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(16), Array(20), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(18), Array(2), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(18), Array(4), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(18), Array(6), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(18), Array(8), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(18), Array(10), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(18), Array(12), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(18), Array(14), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(18), Array(16), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(18), Array(20), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(2,6,10,14,18), Array(), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(2,6,10,14), Array(), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(2,6,10), Array(), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(2,6), Array(), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(4,8,12,16,20), Array(), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(4,8,12,16), Array(), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(4,8,12), Array(), divisorTable, sparkSession)
    divisorTable = addSysCol(Array(4,8), Array(), divisorTable, sparkSession)

    divisorTable.show()

    val stats: ArrayList[Stat] = calculateStats(divisorTable)
    printStats(stats)

    val statsDF: DataFrame = statsArrayListToDF(stats, sparkSession)
    statsDF.show()
    statsDF.write.mode(SaveMode.Overwrite).parquet("./target/stats.parquet")
  }

  /**
   * Gets the max win loss ratio stat
   * @param stats
   * @return
   */
  def projectQuestion1(stats: DataFrame): Any= {
    val maxWinLossRatio = stats.rdd.flatMap(rowToStat).reduce( //for the whole RDD, apply the following function
      (acc, comparee) => {
        val a = comparee.winCount.toDouble / comparee.lossCount
        val b = acc.winCount.toDouble / acc.lossCount
        if (a > b)
          comparee
        else
          acc
      }
    )

    return maxWinLossRatio.winCount.toDouble / maxWinLossRatio.lossCount
  }

  /**
   * Gets the max grand total of the given stats
   * @param stats
   * @return
   */
  def projectQuestion2(stats: DataFrame): Any= {
    var thing: DataFrame = stats.agg(min("grandTotal"), max("grandTotal"))
    var thing2: DataFrame = stats.select(max("grandTotal"))
    var result: Any = thing2.first().get(0)
    return result
  }

  /**
   * Finds how many stats have positive grand totals
   * @param stats
   * @return
   */
  def projectQuestion3(stats: DataFrame): Long = {
    stats.rdd.flatMap(rowToStat).filter((t)=> {t.grandTotal > 0}).count()
  }

  /**
   * Finds how many stats have negative grand totals
   * @param stats
   * @return
   */
  def projectQuestion4(stats: DataFrame): Long = {
    stats.rdd.flatMap(rowToStat).filter((t)=> {t.grandTotal < 0}).count()
  }

  /**
   * Finds how many stats have flat grand totals
   * @param stats
   * @return
   */
  def projectQuestion5(stats: DataFrame): Long = {
    stats.rdd.flatMap(rowToStat).filter((t)=> {t.grandTotal == 0}).count()
  }

  /**
   * Finds the sum of all the grand totals
   * @param stats
   * @return
   */
  def projectQuestion6(stats: DataFrame): Double = {
    stats.rdd.flatMap(rowToStat).reduce((a,b)=> {Stat("",0,0,0,0,0,a.grandTotal+b.grandTotal)}).grandTotal
  }

  /**
   * Gets the average grandTotal for systems containing the number 20
   * @param stats
   * @return
   */
  def projectQuestion7(stats: DataFrame): Double = {
    var thing: (Double, Int) = stats.rdd.flatMap(rowToStat).filter((t)=> {t.colName.contains("20")}).map(stat=>(stat.grandTotal, 1)).reduce(
      (a,b) => {(a._1 + b._1, a._2 + b._2)}
    )

    var avg:Double = thing._1 / thing._2
    avg
  }

  /**
   * Gets the average grandTotal for all systems
   * @param stats
   * @return
   */
  def projectQuestion8(stats: DataFrame): Double = {
    var thing: (Double, Int) = stats.rdd.flatMap(rowToStat).map(stat=>(stat.grandTotal, 1)).reduce(
      (a,b) => {(a._1 + b._1, a._2 + b._2)}
    )

    var avg:Double = thing._1 / thing._2
    avg
  }

  /**
   * Gets the average grandTotal for systems containing the number 20
   * @param stats
   * @return
   */
  def projectQuestion9(stats: DataFrame): Double = {
    var thing: (Double, Int) = stats.rdd.flatMap(rowToStat).filter((t)=> {!t.colName.contains("20")}).map(stat=>(stat.grandTotal, 1)).reduce(
      (a,b) => {(a._1 + b._1, a._2 + b._2)}
    )

    var avg:Double = thing._1 / thing._2
    avg
  }

  /**
   * Adds a system divisor column to the divisorTable
   * @param systemTeamA An array of the columns to compose team A in the system
   * @param systemTeamB An array of the columns to compose team B in the system
   * @param divisorTable The divisor table
   * @param session The spark session
   * @return the new divisor table with the new system column
   */
  def addSysCol(systemTeamA: Array[Int], systemTeamB: Array[Int], divisorTable: DataFrame, session: SparkSession): DataFrame = {
    if(systemTeamA.length == 0 && systemTeamB.length == 0){
      return divisorTable
    }
    var sum: Column = divisorTable.col("zero")
    var frontName: String = ""
    var backName: String = ""
    var systemString: String = ""
    for(i <- 0 to (systemTeamA.length-1)){
      frontName += systemTeamA(i)+"_"
      sum += divisorTable.col(systemTeamA(i).toString) //Add all team A columns
    }
    for(i <- 0 to (systemTeamB.length-1)){
      backName += systemTeamB(i)+"_"
      sum -= divisorTable.col(systemTeamB(i).toString()) //Subtract all team B columns
    }
    systemString = frontName
    if(systemTeamB.length > 0){
      systemString += "vs_"+backName
    }

    val result: DataFrame = divisorTable.withColumn(systemString, sum)
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
    if(cols.length > 3) {//the first three columns are: increment, price, and zero column
      var i: Int = 0
      for (i <- 3 to (cols.length-1)) { //the first three columns are: increment, price, and zero column
        val colName: String = cols(i)
        val dataPoints: RDD[DataPoint] = convertToRddOfDataPoints(systemTable.select("increment", "price", cols(i)))
        val stat: Stat = calculateStat(dataPoints, colName)
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
    printf("system: %20s, winCount: %5d, lossCount: %5d, tieCount: %5d, grandTotal: %5.2f, W/L: %5.2f, L/W: %5.2f \n", stat.colName, stat.winCount, stat.lossCount, stat.tieCount, stat.grandTotal, stat.winCount.toFloat/stat.lossCount, stat.lossCount.toFloat/stat.winCount)
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
      case Row(colName: String, positionPrice: Double, lastPosition: Int, winCount: Int, lossCount: Int, tieCount: Int, grandTotal: Double) => Some(Stat(colName, positionPrice, lastPosition, winCount, lossCount, tieCount, grandTotal))
      case _ => None
    }
  }

  /**
   * Calculates the stats on an RDD of DataPoints: grand total, win count, loss count, etc
   * @param dataPoints The divisor/system column. E.g. a 2 day split moving average column
   * @param colName The name of the column stats are being calculated for. To show which system had which stats.
   * @return
   */
  def calculateStat(dataPoints: RDD[DataPoint], colName: String): Stat = {
    val baseVal = new Stat("",0, 0,0, 0, 0, 0)

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

      val newCalc = new Stat(colName, positionPrice, positionNow, winCount, lossCount, tieCount, grandTotal)
      newCalc
    } //takes a item and a seq, returns a seq

    val comboOp = (a: Stat, b: Stat) => {
      val newStats = new Stat(a.colName + b.colName, 0,0, a.winCount + b.winCount, a.lossCount + b.lossCount, a.tieCount + b.tieCount, a.grandTotal + b.grandTotal)
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
        .withColumn("2", splitMA2(col("price"), lit(2)).over(Window.orderBy("increment").rowsBetween(-1, 0)))
        .withColumn("4", splitMA4(col("price"), lit(4)).over(Window.orderBy("increment").rowsBetween(-3, 0)))
        .withColumn("6", splitMA6(col("price"), lit(6)).over(Window.orderBy("increment").rowsBetween(-5, 0)))
        .withColumn("8", splitMA8(col("price"), lit(8)).over(Window.orderBy("increment").rowsBetween(-7, 0)))
        .withColumn("10", splitMA10(col("price"), lit(10)).over(Window.orderBy("increment").rowsBetween(-9, 0)))
        .withColumn("12", splitMA12(col("price"), lit(12)).over(Window.orderBy("increment").rowsBetween(-11, 0)))
        .withColumn("14", splitMA14(col("price"), lit(14)).over(Window.orderBy("increment").rowsBetween(-13, 0)))
        .withColumn("16", splitMA16(col("price"), lit(16)).over(Window.orderBy("increment").rowsBetween(-15, 0)))
        .withColumn("18", splitMA18(col("price"), lit(18)).over(Window.orderBy("increment").rowsBetween(-17, 0)))
        .withColumn("20", splitMA20(col("price"), lit(20)).over(Window.orderBy("increment").rowsBetween(-19, 0)))

    newDF.orderBy("increment").show()
    return newDF
  }
}
