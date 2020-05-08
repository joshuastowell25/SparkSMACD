# SMACD

The six most important files for this project are:
src/main/scala/com/spark/smacd/SmacdDriver.scala
src/main/scala/com/spark/smacd/SplitMA.scala
src/main/scala/com/spark/smacd/DataPoint.scala
src/main/scala/com/spark/smacd/Stat.scala
src/test/scala/com/spark/project/SmacdDriverTest.scala
data/sp1985.csv


## Project Overview
This project is a demonstration of how to calculate results of split moving average convergence divergence systems on S&P500 daily historical closing price data from 1985-2020. A split moving average is the most sum of the most recent x number of days, minus the sum of the x number of days before it. E.g. for a 10 day split moving average, you would sum up the last 5 days and subtract the sum of the 5 days before those.

## Code Overview

### Driver

`SmacdDriver` is the Spark driver that coordinates calculations. It's main class can be run to calculate the Stats from the sp500 data and the given systems. First it creates a divisor table, where the split moving average divisors are calculated. These include the 2 day, 4 day, 6 day, etc. all the way up to the 20 day split moving average. The calculations are done using the `SplitMA` class which extends a UDAF. This allows for the split moving average calculation do be done over any given window.

### Test

`SmacdDriverTest` contains nine tests for various questions such as: which system had the highest grand total, which system had the best win/loss ratio, etc. 
