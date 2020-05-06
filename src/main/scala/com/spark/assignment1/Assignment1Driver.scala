package com.spark.assignment1

import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

object Assignment1Driver {
  /**
    * Helper function to print out the contents of an RDD
    * @param label Label for easy searching in logs
    * @param theRdd The RDD to be printed
    * @param limit Number of elements to print
    */
  private def printRdd[_](label: String, theRdd: RDD[_], limit: Integer = 20) = {
    val limitedSizeRdd = theRdd.take(limit)
    println(s"""$label ${limitedSizeRdd.toList.mkString(",")}""")
  }

  //Find the trip with the longest trip duration.
  //Return that duration.
  //How this problem is being solved: uses reduce to iterate over RDD and find max duration
  def problem1(tripData: RDD[Trip]): Long = {
    val maxDurationTrip = tripData.reduce( //for the whole RDD, apply the following function
      (acc, comparee) => {
        val a = comparee.duration
        val b = acc.duration
        if (a > b)
          comparee
        else
          acc
      }
    )
    return maxDurationTrip.duration
  }

  //find the count of the trips where start_station == "San Antonio Shopping Center"
  //How this problem is being solved: filters RDD by column then uses count
  def problem2(trips: RDD[Trip]): Long = {
    trips.filter((t) => { t.start_station == "San Antonio Shopping Center" }).count()
  }

  //How this problem is being solved: uses aggregate and sequences to get all subscriber types
  def problem3(trips: RDD[Trip]): Seq[String] = {
    val baseVal = Seq[String]() //empty seq
    val seqOp = (list: Seq[String], item: Trip) => (list :+ item.subscriber_type) //takes a item and a seq, returns a seq
    val comboOp = (a: Seq[String], b: Seq[String]) => (a ++ b) //puts two seqs together
    trips.aggregate(baseVal)(seqOp, comboOp)
  }

  //find the busiest zipcode
  import scala.collection.mutable.HashMap

  //How this problem is being solved: uses aggregate. builds a hashmap for zipcode and counts. merging them in the combine operation.
  //grabs the max from the hashmap later
  def problem4(trips: RDD[Trip]): String = {

    //accumulation, element
    def seqOp(map: mutable.HashMap[String, Int], trip: Trip): mutable.HashMap[String, Int] = {
      if (map.contains(trip.zip_code)) {
        map.put(trip.zip_code, map.get(trip.zip_code).get + 1)
      } else {
        map.put(trip.zip_code, 1)
      }
      return map
    }

    //mutable.HashMap[String, Int]
    def comboOp(a: mutable.HashMap[String, Int], b: mutable.HashMap[String, Int]): mutable.HashMap[String, Int] = { //puts two seq outputs together
      var aKeys = a.keySet
      for (k <- aKeys) {
        var va: Int = a.get(k).get
        var vb: Int = 0
        if (b.contains(k)) {
          vb = b.get(k).get
        }
        var combined: Int = va + vb
        b.update(k, combined)
      }
      return b
    }

    //return trips.aggregate {new mutable.HashMap()}(seqOp, comboOp)
    var base: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()
    var aggregation: mutable.HashMap[String, Int] = trips.aggregate(base)(seqOp, comboOp)
    var maxTuple = aggregation.maxBy { case (key, value) => value } //max by value
    return maxTuple._1
  }

  //count how many trips have an end date on a different date
  //How this problem is being solved: maps a date checking function where 1 is returned when start and end dates differ
  def problem5(trips: RDD[Trip]): Long = {

    val reduced = trips
      .map(f => {
        val startSplit = f.start_date.split(" ")
        val endSplit = f.end_date.split(" ")

        if (startSplit(0) != endSplit(0)) {
          1
        } else {
          0
        }
      })
      .reduce((u1: Int, u2: Int) => (u1 + u2))

    return reduced
  }

  //How this problem is being solved: Uses map reduce to get a count on trips
  def problem6(trips: RDD[Trip]): Long = {
    val reduced = trips
      .map(f => {
        1
      })
      .reduce((u1: Int, u2: Int) => (u1 + u2))

    return reduced
  }

  //How this problem is being solved: checks the date in the start_date, reduces to a tuple of # overnight and #total
  def problem7(trips: RDD[Trip]): Double = {
    val reduced = trips
      .map(f => {
        val startSplit = f.start_date.split(" ")
        val endSplit = f.end_date.split(" ")

        if (startSplit(0) != endSplit(0)) {
          (1, 1) //overnight, total
        } else {
          (0, 1) //overnight, total
        }
      })
      .reduce((u1: Tuple2[Int, Int], u2: Tuple2[Int, Int]) => ((u1._1 + u2._1, u1._2 + u2._2))) //920/354152=.0025
    return reduced._1.toFloat / reduced._2.toFloat
  }

  //How this problem is being solved: maps a doubling function over the trips
  def problem8(trips: RDD[Trip]): Double = {
    val reduced = trips
      .map(item => {
        item.duration * 2
      })
      .reduce((u1, u2) => u1 + u2)
    return reduced
  }

  //How this problem is being solved: calculates start terminal for tripid 913401. grabs the first station from that terminal. returns stations coordinates.
  def problem9(trips: RDD[Trip], stations: RDD[Station]): (Double, Double) = {
    val terminal = trips.filter(f => (f.trip_id.equalsIgnoreCase("913401"))).collect()(0).start_terminal
    val station = stations.filter(s => (s.station_id == terminal)).collect()(0)
    return (station.lat, station.lon)
  }

  //stationid, sumduration of all trips starting there.
  //("San Antonio Shopping Center",2937220)
  //How this problem is being solved: joins the Rdd's maps duration, reduces by key summing duration, collect to array
  def problem10(trips: RDD[Trip], stations: RDD[Station]): Array[(String, Long)] = {
    //Better way (only needs trips): trips.keyBy(trip=>trip.start_station).mapValues(f=>f.duration).reduceByKey((a,b)=> a+b).collect()
    trips
      .keyBy(trip => trip.start_station)
      .join(stations.keyBy(station => station.name))
      .mapValues(f => f._1.duration)
      .reduceByKey((a, b) => a + b)
      .collect()
  }

  /*
     Dataframes
   */

  //How this problem is being solved: Uses select to select the trip_id column
  def dfProblem11(trips: DataFrame): DataFrame = {
    return trips.select(col("trip_id"))
  }

  //   * Count all the trips starting at 'Harry Bridges Plaza (Ferry Building)'
  //How this problem is being solved: Uses dataframe where start_station is the desired start_station
  def dfProblem12(trips: DataFrame): DataFrame = {
    trips.where("start_station == 'Harry Bridges Plaza (Ferry Building)'")
  }

  //SUM duration of all trips
  //How this problem is being solved: Selecting the first duration
  def dfProblem13(trips: DataFrame): Long = {
    return trips.select(sum("duration")).first().getLong(0)
  }
}
