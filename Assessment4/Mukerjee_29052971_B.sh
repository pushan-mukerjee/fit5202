#!/bin/bash

#removing the csv directories created by PARTB if they exist
bash -c '[ -d /tmp/29052971/29052971_B_1.csv ] && /tmp/29052971/rm -r 29052971_B_1.csv'
bash -c '[ -d 29052971_B_2.csv ] && rm -r /tmp/29052971/29052971_B_2.csv'
bash -c '[ -d 29052971_B_3.csv ] && rm -r /tmp/29052971/29052971_B_3.csv'
bash -c '[ -d 29052971_B_4.csv ] && rm -r /tmp/29052971/29052971_B_4.csv'

echo "

//importing necessary libraries
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.hashing.MurmurHash3
import org.apache.spark.SparkContext._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import scala.util.Random
import org.apache.spark._
import org.apache.spark.streaming._
import java.io.{ PrintWriter, File, FileOutputStream }


/* PART A */
println(\"PART A START\")

//configurable parameters
val fileInterval = 5 //interval in seconds for generating a file
val n = 1 //initialising the file counter 

val r = new scala.util.Random(2) //defines random number generator
val start = 10 //lower bound for random number generator 
val end = 99 //upper bound for random number generator

//generates 100 random numbers between start and end
val pwText = new PrintWriter( new File(\"/tmp/29052971/stream_\" + n + \".txt\"))
val stream = Seq.fill(100)(start + r.nextInt( (end - start) + 1))
stream.size
val line = stream.mkString(\",\")
println(line)
pwText.write(line + \"\n\") //writes the line to a file

//Streaming a text file containing a stream of 2 dimensional digits
/* NOTE: This does not work so code shown here for consideration */

/*
//configurable parameters
val fileInterval = 5 //interval in seconds for streaming a file

//defines a Spark Configuration and Spark Streaming Context for streaming every interval defined by <fileInterval>
val conf = new SparkConf().setAppName(\"Reading Stream Input\");
conf.set(\"spark.driver.allowMultipleContexts\", \"true\");
conf.setMaster(\"local\");

//val new SparkContext(conf)
val ssc = new StreamingContext(conf, Seconds(fileInterval))
*/

//loading the stream data in the text file containing 2 dimensional digits 
val streamData = sc.textFile(\"/tmp/29052971/stream.txt\")

//parsing the stream data in the text file into a Vector
//(each item is mapped to a Double)
val parsedData = streamData.map(x => Vectors.dense(x.split(',').map(_.toDouble))).cache()

parsedData.collect()
 
// Cluster the data into N classes using KMeans
val numClusters = 3
val numIterations = 3
val clusters = KMeans.train(parsedData, numClusters, numIterations)

// Evaluate clustering by computing Within Set Sum of Squared Errors
val WSSSE = clusters.computeCost(parsedData)
println(s\"Within Set Sum of Squared Errors = $WSSSE\")

//ssc.stop() // if streaming was working 

/* PART A END */
println(\"PART A END\")


/* PART B */
println(\"PART B START\")

/* Timer */
def time[R](block: => R): R = {
          val t0 = System.nanoTime()
          val result = block    // call-by-name
          val t1 = System.nanoTime()
          println(\"Elapsed time: \" + (t1 - t0) + \"ns\")
          result
}

//Load the test.csv.bz dataset from /tmp/29052971/test.csv.bz2
val df = spark.read.option(\"header\", \"false\").csv(\"/tmp/29052971/test.csv.bz2\")

//print the dataset columns
df.printSchema()

//Create the Graph Vertices, (VertexId, AirportCode) tuples, which are the unique Airport Codes from OriginAirport (_c6) and DestinationAirport (_c8) fields 

val airportVertices: RDD[(VertexId, String)] = df.select($\"_c6\", $\"_c8\").flatMap(airportCode => Iterable(airportCode(0).toString, airportCode(1).toString)).rdd.distinct().map(airportCode => (MurmurHash3.stringHash(airportCode), airportCode))


//Creates dataframe with the necessary raw data origin, destination, acrualDeparturetime, actualArrivalTime, departureDelay, arrivalDelay 

val flightsOriginDest = df.select($\"_c6\".alias(\"OriginAirport\"), $\"_c8\".alias(\"DestinationAirport\"), $\"_c10\".alias(\"ActualDepartureTime\"), $\"_c11\".alias(\"DepartureDelay\"), $\"_c13\".alias(\"ActualArrivalTime\"), $\"_c16\".alias(\"ArrivalDelay\")) 

//creates the AirTime column from ActualArrivalTime - ActualDepartureTime

val flightsOriginFinalDf = flightsOriginDest.withColumn(\"ActualAirTime\", flightsOriginDest(\"ActualArrivalTime\") - flightsOriginDest(\"ActualDepartureTime\"))

//creates a final flights dataframe with attributes: originAirport, destinationAirport, DepartureDelay, ArrivalDelay, ActualAirTime for attribute extraction later in order to create edges for the graph.

val flightsDf = flightsOriginFinalDf.select($\"OriginAirport\", $\"DestinationAirport\", $\"DepartureDelay\".cast(\"integer\"), $\"ArrivalDelay\".cast(\"integer\"), $\"ActualAirTime\".cast(\"integer\"))

flightsDf.printSchema()

flightsDf.show()

/* Create default airport for Graph */
val defaultAirport = (\"Missing\") 

/* 
Creates individual RDDs for the Edge attributes to be modelled: numFlights, DepartureDelay, ArrivalDelay and AirTime. Totals will be extracted first so that averages can be calculated later by joining all the RDDs. The summarized Average data for each flight attribute will be put into a dataframe and modelled as Edge attributes for the graph.
*/ 


//Creates an RDD that maps the number of flights for each origin,dest pair (starting with count 1) and then reduces to get the total number of flights per (origin, destination) pair. The result is an RDD of form (origin,destination,numFlights)

val numFlightsRDD = flightsDf.map(x => ((x(0).toString,x(1).toString), 1)).rdd.reduceByKey(_+_)

//Creates an RDD that maps the Departure Delay for each origin,dest pair (flight) and then reduces to get the total Departure Delay per (origin, destination) pair. The result is an RDD of form (origin,destination,TotalDepartureDelay)

val depDelayRDD = flightsDf.map(x => ((x(0).toString,x(1).toString), x(2).asInstanceOf[Int])).rdd.reduceByKey(_+_)

//Creates an RDD that maps the Arrival Delay for each origin,dest pair (flight) and then reduces to get the total Arrival Delay per (origin, destination) pair. The result is an RDD of form (origin,destination,TotalArrivalDelay) 

val arrDelayRDD = flightsDf.map(x => ((x(0).toString,x(1).toString), x(3).asInstanceOf[Int])).rdd.reduceByKey(_+_)

//Creates an RDD that maps the Air Time for each origin,dest pair (flight) and then reduces to get the total Airtime per (origin, destination) pair. The result is an RDD of form (origin,destination,TotalAirTime) 

val airTimeRDD = flightsDf.map(x => ((x(0).toString,x(1).toString), x(4).asInstanceOf[Int])).rdd.reduceByKey(_+_)

//Creates an edgeRDD that joins all the attribute RDDs to create a set of key-value pairs of the form (origin, dest), (((numFlights, TotalDepartureDelay, TotalArrivalDelay, TotalAirTime). Then maps the tuples to the form (origin, dest), (numFlights, AverageDepDelay, AverageArrivalDelay, AverageAirTime). During the mapping the averages are calculated by dividing each Total by the numFlights counter. 

val edgeRDD = numFlightsRDD.join(depDelayRDD).join(arrDelayRDD).join(airTimeRDD).map{case ((origin, dest), (((count, depDelay), arrDelay), airTime)) => ((origin, dest), (count, depDelay/count, arrDelay/count, airTime/count))}

val mappedEdgeRDD = edgeRDD.map(x => (x._1._1, x._1._2, x._2._1, x._2._2, x._2._3, x._2._4))

//Creates an edgeDf dataframe from the mappedEdgeRDD that stores all the Average metrics for each flight/traffic route identified by originAirport ad destinationAirport

val edgeDf = spark.createDataFrame(mappedEdgeRDD).toDF(\"OriginAirport\", \"DestinationAirport\", \"NumFlights\", \"AvgDepartureDelay\", \"AvgArrivalDelay\", \"AvgAirTime\")

//print the schema of the edge dataframe which will be used to create the edges

edgeDf.printSchema()

//shows the first 10 rows of data from the edge dataframe with Aveerage metrics

edgeDf.show()

//creates a flightsFromTo dataframe that selects all the attributes from the EdgeDf dataframe for Edge creation in order to model each flight/traffic route

val flightsFromTo = edgeDf.select($\"OriginAirport\",$\"DestinationAirport\",$\"NumFlights\",$\"AvgDepartureDelay\",$\"AvgArrivalDelay\",$\"AvgAirTime\")

//creates a case class called EdgeWeight to store all the attributes for each flight/traffic route (modelled as Edges). The attributes modelled per route are: numberofFlights, AvgDepartureDelay, AvgArrivalDelay, and AvgAirTime. This data is sourced from the flightsFromTo dataframe created above with all the Average metrics pre-calculated.

case class EdgeWeight(NumFlights: String, AvgDepDelay: String, AvgArrDelay: String, AvgAirTime: String) 

//Reads the flightsFromTo dataframe and creates the flight edges by hashing the origin airport and destination airport to create Vertex Id's. Then creates a tuple 3 consisting of (originAirportVertexId, DestAirportVertexId, EdgeWeight(numFlights, AvgDepartureDelay, AvgArrivalDelay, AvgAirTime)). Convert tuple3 into an RDD and maps each tuple3 to an Edge object.

val flightEdges = flightsFromTo.map(x => ((MurmurHash3.stringHash(x(0).toString), MurmurHash3.stringHash(x(1).toString)), EdgeWeight(x(2).toString, x(3).toString, x(4).toString, x(5).toString))).rdd.map(x => Edge(x._1._1,x._1._2,x._2))

//creates the graph from the airportVertices, flightEdges and defaultAiport
val graph = Graph(airportVertices, flightEdges, defaultAirport)

//prints number of vertices
graph.numVertices

//prints number of edges
graph.numEdges

/* Q1. Top 10 Busiest Airports */
println(\"Q1. Top 10 Busiest Airports\")

//Maps the airport pairs to (originVertexId, numFlights) and (destVertexId, numFlights) tuples, unions them to remove overlapping tuples which creates a set of (airport, numFlights) tuples. Then reduces the set of tuples which sums the numFlights across each airport key. This gives a set of unique (airport, totalFlights) tuples. These are sorted by totalFlights (2nd attribute) from greatest to least and the top 10 with greatest Total flights are extracted. 

val topTenBusiestAirports = graph.triplets.map(triplet => (triplet.srcAttr, triplet.attr.NumFlights.toFloat)).union(graph.triplets.map(triplet => (triplet.dstAttr, triplet.attr.NumFlights.toFloat))).reduceByKey(_+_).sortBy(_._2, ascending=false).take(10)

//time the run for Q1.
time{topTenBusiestAirports.take(10)}

//print to csv
val topTenBusiestAirportsDf = sc.parallelize(topTenBusiestAirports).toDF(\"Airport\",\"TotalFlights\").coalesce(1).write.format(\"com.databricks.spark.csv\").option(\"header\", \"true\").save(\"/tmp/29052971/29052971_B_1.csv\")

/* Q2. Top 10 Longest Airline Routes */
println(\"Q2. Top 10 Longest Airline Routes\")

//Maps the graph triplets to (originAirport, destinationAirport, AvgAirTime) tuple3. This provides each unique route and their Average Air Time. The tuple3 triplets are sorted by Average Air Time from greatest to least and the top 10 extracted. 

val topTenLongestRoutes = graph.triplets.map(triplet => (triplet.srcAttr, triplet.dstAttr, triplet.attr.AvgAirTime)).sortBy(_._3, ascending=false).take(10)

//time the run for Q2.
time{topTenLongestRoutes.take(10)}

//print to csv
val topTenLongestRoutesDf = sc.parallelize(topTenLongestRoutes).toDF(\"OriginAirport\",\"DestinationAirport\",\"AvgAirTime\").coalesce(1).write.format(\"com.databricks.spark.csv\").option(\"header\", \"true\").save(\"/tmp/29052971/29052971_B_2.csv\")


/* Q3. Top 10 Worst Routes based on longest Average Arrival Delay */
println(\"Q3. Top 10 Worst Airline Routes\")

//Maps the graph triplets to (originAirport, destinationAirport, AvgArrivalDelay) tuple3. This provides each unique route and their Average Arrival Delay. The tuple3 triplets are sorted by Average Arrival Delay from greatest to least and the top 10 extracted. 

val topTenWorstRoutes = graph.triplets.map(triplet => (triplet.srcAttr, triplet.dstAttr, triplet.attr.AvgArrDelay)).sortBy(_._3, ascending=false).take(10)

//time the run for Q3.
time{topTenWorstRoutes.take(10)}

//print to csv
val topTenWorstRoutesDf = sc.parallelize(topTenWorstRoutes).toDF(\"OriginAirport\",\"DestinationAirport\",\"AvgArrivalDelay\").coalesce(1).write.format(\"com.databricks.spark.csv\").option(\"header\", \"true\").save(\"/tmp/29052971/29052971_B_3.csv\")


/* Q4. Top 10 Worst Airports based on longest Average Departure Delay */
println(\"Q4. Top 10 Worst Airports\")

//Step 1. Counts outgoing egdes of each airport vertex to calc number of departure routes and outputs a set of (airport, numRoutes) tuples

val numDepartureRoutes = graph.outDegrees.join(airportVertices).map(x => (x._2._2, x._2._1.toFloat))

//Step 2. Calculates the sum AvgDepartureDelay per Departure airport by summing the AvgDepartureDelay (across all outgoing edges) for each origin airport (source vertex)

val sumAvgDepDelay = graph.triplets.map(triplet => (triplet.srcAttr, triplet.attr.AvgDepDelay.toFloat)).reduceByKey(_+_)

//Step 3. Joins the number of outgoing departure routes with the total AvgDepDelay per departure aiport. This gives a key-value pair in the form (originAirport, (numDepartureRoutes, TotalAvgDepartureDelay)). The Total AvgDepartureDelay per departure airport is divided by the number of outgoing departure routes to get the Average Departure Delay across all outgoing routes, per departure airport. The result is sorted from greatest to least to see the top 10 departure airports with greatest Average Depature Delay across its outgoing routes.

val topTenWorstAirports = numDepartureRoutes.join(sumAvgDepDelay).map(x => (x._1, x._2._2/x._2._1)).sortBy(_._2, ascending=false).take(10)

//timing the run for Q4
time{topTenWorstAirports.take(10)}

//print to csv
val topTenWorstAirportsDf = sc.parallelize(topTenWorstAirports).toDF(\"OriginAirport\",\"AvgDepartureDelay\").coalesce(1).write.format(\"com.databricks.spark.csv\").option(\"header\", \"true\").save(\"/tmp/29052971/29052971_B_4.csv\")


/* End Part B */
println(\"PART B END\")

/* Part C START */
println(\"PART C START\")

/* Q1 */
println(\"Q1\")

//define function to convert a dataframe column to Double Type
val toDouble = udf[Double, String]( _.toDouble)

//creating a dataframe from the loaded df with necessary columns 

val initDf = df.select($\"_c0\".alias(\"DayOfMonth\"), $\"_c1\".alias(\"DayOfWeek\"), $\"_c4\".alias(\"FlightNum\"), $\"_c5\".alias(\"OriginAirportID\"), $\"_c7\".alias(\"DestinationAirportID\"), $\"_c9\".alias(\"ScheduledDepartureTime\"), $\"_c11\".alias(\"DepartureDelay\"), $\"_c14\".alias(\"ArrivalDelay\")) 

//creates the OverallDelay column from ArrivalDelay - DepartureDelay, and then drops the redundant DepartureDelay and ArrivalDelay columns

val initDf2 = initDf.withColumn(\"OverallDelay\", initDf(\"ArrivalDelay\") - initDf(\"DepartureDelay\")).drop(\"ArrivalDelay\").drop(\"DepartureDelay\").sort(desc(\"FlightNum\"), asc(\"ScheduledDepartureTime\"))

//creates the flightModelDf dataframe containing the predictor parameters that will be features in the lr model, as well as the predicted parameter (OverallDelay) 

val flightModelDf = initDf2.withColumn(\"DayOfMonth\", toDouble(initDf2(\"DayOfMonth\"))).withColumn(\"DayOfWeek\", toDouble(initDf2(\"DayOfWeek\"))).withColumn(\"FlightNum\", toDouble(initDf2(\"FlightNum\"))).withColumn(\"OriginAirportID\", toDouble(initDf2(\"OriginAirportID\"))).withColumn(\"DestinationAirportID\", toDouble(initDf2(\"DestinationAirportID\"))).withColumn(\"ScheduledDepartureTime\", toDouble(initDf2(\"ScheduledDepartureTime\")))

//Displays the schema and first 20 rows of the final flightModel dataframe
flightModelDf.printSchema()
flightModelDf.show()

/* Q2 */
println(\"Q2\")

//Adds rowId to the FlightModel dataframe to allow seperation of test and train sets

val adjustedModelDf = flightModelDf.withColumn(\"rowId\", monotonically_increasing_id())

//Creates the test set with rowIds that are multiple of 10
val n = 10 //used to filter out every 10th row into the test set
val testDf = adjustedModelDf.filter($\"rowId\" % n === 0)
testDf.show()


//creates the training set with rowIds that are not multiple of 10
val trainDf = adjustedModelDf.filter($\"rowId\" % n > 0)
trainDf.show()

/* Q3 */
println(\"Q3\")

//Refines the Training Set - drops rowId 
val trainingSet = trainDf.drop(\"rowId\") 
trainingSet.show()

/*
Fits a linear regression model (estimator) with the training data set to predict overall delay with the variables in the adjustedModelDf: DayOfMonth, DayOfWeek, FlightNum, OriginAirportID, DestinationAirportID, ScheduledDepartureTime 
*/

//creating the feature vector assembler to create feature vectors/predictors for the model 
val flightAssembler = new VectorAssembler()

//define the Feature Vectors from adjustedModelDf ie. predictor variables
flightAssembler.setInputCols(Array(\"DayOfMonth\", \"DayOfWeek\", \"FlightNum\", \"OriginAirportID\", \"DestinationAirportID\", \"ScheduledDepartureTime\"))

//create the feature vector column with predictor variables from adjustedModelDf
flightAssembler.setOutputCol(\"features\")

//run the assembler to transform the training set into the feature vector column
val trainOutput = flightAssembler.transform(trainingSet)

//Creating the Estimator - Linear Regression Model
val lr = new LinearRegression()
lr.setMaxIter(10) //setting 10 iterations
lr.setRegParam(0.3)
lr.setElasticNetParam(0.8)
lr.setLabelCol(\"OverallDelay\") //setting the Predicted Column as OverallDelay
lr.setFeaturesCol(\"features\") //setting the features column

//Training the model by fitting the lr model into the training data set
val trainModel = lr.fit(trainOutput) 

//Refines the Test Set - drops rowId 
val testSet = testDf.drop(\"rowId\") 
testSet.show()

//run the assembler to transform the test set into the feature vector column
val testOutput = flightAssembler.transform(testSet)

//Fitting the model to the test data
val testModel = lr.fit(testOutput) 

/* Q4 */
println(\"Q4\")

//reporting the RMSE on training set 
val modelSummary = trainModel.summary
modelSummary.rootMeanSquaredError

//reporting the RMSE on Test Data
val testModelSummary = testModel.summary
testModelSummary.rootMeanSquaredError

/* Q5 */
println(\"Q5\")

//Reporing the flightModel's Intercept and Co-efficients for the training Set
trainModel.coefficients //parameter co-efficients (features)
trainModel.intercept //intercept


/* End Part C */
println(\"PART C END\")


" | spark-shell
