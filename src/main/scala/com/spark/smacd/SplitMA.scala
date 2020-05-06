package com.spark.smacd

import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{StructField, _}

class SplitMA extends UserDefinedAggregateFunction {

  //Weird way to get enums in Scala
  object maCalcFields extends Enumeration
  {
    type Main = Value
    val result = Value(0)
  }

  //Weird way to get enums in Scala
  object inputFields extends Enumeration
  {
    type Main = Value
    val price = Value(0)
    val maDivisor = Value(1)
  }

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    StructField("result", DoubleType) :: Nil //args: type and containsNull
    //:: Nil goes after the LAST struct field
  )

  // This is the output type of your aggregation function.
  override def dataType: DataType = DoubleType

  //required by the UDAF interface
  override def deterministic: Boolean = true

  // This is the initial value for your buffer.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(maCalcFields.result.id) = 0.0
  }

  // This is the input schema for your aggregate function's update method
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(
      StructField("price", DoubleType) ::
      StructField("divisor", IntegerType) :: Nil
    )

  //used to carry intermediate values for the split moving average calculation
  var carrier: util.ArrayList[Double] = new util.ArrayList[Double]()

  // This is how to update your buffer schema given an input.
  //overridden from UserDefinedAggregateFunction
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val divisor: Int = input.getAs[Int](inputFields.maDivisor.id)
    val price: Double = input.getAs[Double](inputFields.price.id)
    var frontSum: Double = 0
    var backSum: Double = 0
    val halfLength: Int = divisor/2

    if(carrier.size() < divisor){ //we are in the beginning of the list and there aren't enough elements in the carrier
      carrier.add(price)
    }else{
      carrier.remove(0) //pop the first element
      carrier.add(price) //push the next element

      //do the split MA sum calculations
      for( i <- 0 to halfLength - 1){
        backSum += carrier.get(i)
        frontSum += carrier.get(i + halfLength)
      }
    }

    buffer(maCalcFields.result.id) = frontSum - backSum
  }

  // This is how to merge two objects with the bufferSchema type. (they merge into buffer1)
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {

  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    //front avg minus back avg
    buffer.getDouble(maCalcFields.result.id)
  }
}