package com.spark.smacd

case class Stat(
    positionPrice: Double,
    lastPosition: Int,
    winCount: Int,
    lossCount: Int,
    tieCount: Int,
    grandTotal: Double
)
