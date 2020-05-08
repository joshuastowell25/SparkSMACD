# SMACD

The six most important files for this project are:
src/main/scala/com/spark/smacd/SmacdDriver.scala
src/main/scala/com/spark/smacd/SplitMA.scala
src/main/scala/com/spark/smacd/DataPoint.scala
src/main/scala/com/spark/smacd/Stat.scala
src/test/scala/com/spark/project/SmacdDriverTest.scala
data/sp1985.csv


## Project Overview
This project is a demonstration of how to calculate results of split moving average convergence divergence (SMACD) systems on S&P500 daily historical closing price data from 1985-2020. A split moving average is the most sum of the most recent x number of days, minus the sum of the x number of days before it. E.g. for a 10 day split moving average, you would sum up the last 5 days and subtract the sum of the 5 days before those.

## Code Overview

### Driver

`SmacdDriver` is the Spark driver that coordinates calculations. It's main class can be run to calculate the Stats from the sp500 data and the given systems. First it creates a divisor table, where the split moving average divisors are calculated. These include the 2 day, 4 day, 6 day, etc. all the way up to the 20 day split moving average. The calculations are done using the `SplitMA` class which extends a UDAF. This allows for the split moving average calculation do be done over any given window.

### Test

`SmacdDriverTest` contains nine tests for various questions such as: which system had the highest grand total, which system had the best win/loss ratio, etc. 

### Others

`DataPoint` is a case class used to hold smacd divisor calculations, it contains increment, price, and divVal. increment is just the sequence number for next field, the price. Price is the closing price of the sp500 that day, and divVal is the calculation of the SMACD divisor or system at that increment. For example, if on the 100th day the 10 day split MA is 933.25, then increment 100 would have divVal equal to 933.25 the dataPoint representing the 10 day Split moving average (SMA). Other datapoints may represent another column in the divisor calculations table, such as a system column. One example of a 'system' may be the 10 day SMA vs the 20 day SMA, in which case the divVal at increment x would be the divVal for the 10 day minus the divVal for the 20 day. 

You will see in SmacdDriver.scala that defining moving average systems is very easy:
  divisorTable = addSysCol(Array(2), Array(4), divisorTable, sparkSession) //2 day vs 4 day SMACD system

