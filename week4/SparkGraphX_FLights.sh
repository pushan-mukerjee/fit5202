!#/bin/bash

scala> val df = spark.read.option("header", "true").csv("/home/user/Documents/Datasets/flights_2008.csv.bz2")
18/06/03 19:12:19 WARN ObjectStore: Failed to get database global_temp, returning NoSuchObjectException
df: org.apache.spark.sql.DataFrame = [Year: string, Month: string ... 27 more fields]

scala> df.printSchema()
root
 |-- Year: string (nullable = true)
 |-- Month: string (nullable = true)
 |-- DayofMonth: string (nullable = true)
 |-- DayOfWeek: string (nullable = true)
 |-- DepTime: string (nullable = true)
 |-- CRSDepTime: string (nullable = true)
 |-- ArrTime: string (nullable = true)
 |-- CRSArrTime: string (nullable = true)
 |-- UniqueCarrier: string (nullable = true)
 |-- FlightNum: string (nullable = true)
 |-- TailNum: string (nullable = true)
 |-- ActualElapsedTime: string (nullable = true)
 |-- CRSElapsedTime: string (nullable = true)
 |-- AirTime: string (nullable = true)
 |-- ArrDelay: string (nullable = true)
 |-- DepDelay: string (nullable = true)
 |-- Origin: string (nullable = true)
 |-- Dest: string (nullable = true)
 |-- Distance: string (nullable = true)
 |-- TaxiIn: string (nullable = true)
 |-- TaxiOut: string (nullable = true)
 |-- Cancelled: string (nullable = true)
 |-- CancellationCode: string (nullable = true)
 |-- Diverted: string (nullable = true)
 |-- CarrierDelay: string (nullable = true)
 |-- WeatherDelay: string (nullable = true)
 |-- NASDelay: string (nullable = true)
 |-- SecurityDelay: string (nullable = true)
 |-- LateAircraftDelay: string (nullable = true)


scala> val airportCodes = df.select($"Origin", $"Dest").flatMap(x => Iterable(x(0).toString, x(1).toString))
airportCodes: org.apache.spark.sql.Dataset[String] = [value: string]

scala> airportCodes.take(20)
18/06/03 19:23:54 WARN Utils: Truncated the string representation of a plan since it was too large. This behavior can be adjusted by setting 'spark.debug.maxToStringFields' in SparkEnv.conf.
res2: Array[String] = Array(IAD, TPA, IAD, TPA, IND, BWI, IND, BWI, IND, BWI, IND, JAX, IND, LAS, IND, LAS, IND, MCI, IND, MCI)

scala> airportCodes.count()
res3: Long = 14019456                                                           
scala> airportCodes.distinct.count()
res4: Long = 305                                                                
scala> val airportVertices: RDD[(VertexId, String)] = airportCodes.rdd.distinct().map(x => (MurmurHash3.stringHash(x), x))
airportVertices: org.apache.spark.rdd.RDD[(org.apache.spark.graphx.VertexId, String)] = MapPartitionsRDD[43] at map at <console>:35


val flightEdges = flightsFromTo.map(x => ((MurmurHash3.stringHash(x(0).toString), MurmurHash3.stringHash(x(1).toString)), 1))

scala> val flightEdges = flightsFromTo.map(x => ((MurmurHash3.stringHash(x(0).toString), MurmurHash3.stringHash(x(1).toString)), 1))
flightEdges: org.apache.spark.sql.Dataset[((Int, Int), Int)] = [_1: struct<_1: int, _2: int>, _2: int]

scala> val flightEdges = flightsFromTo.map(x => ((MurmurHash3.stringHash(x(0).toString), MurmurHash3.stringHash(x(1).toString)), 1)).rdd.reduceByKey(_+_).map(x => Edge(x._1._1,x._1._2,x._2))
flightEdges: org.apache.spark.rdd.RDD[org.apache.spark.graphx.Edge[Int]] = MapPartitionsRDD[65] at map at <console>:35

scala> flightEdges.take(10)
res8: Array[org.apache.spark.graphx.Edge[Int]] = Array(Edge(1095539962,178004914,2205), Edge(1567376521,1710100676,925), Edge(-1576863504,52301407,58), Edge(1917476200,813811672,3729), Edge(-1928985797,-216730884,1379), Edge(-1020692585,1058030826,94), Edge(-1787161043,178004914,509), Edge(1226329766,1146947515,2), Edge(-1515187592,1710100676,3575), Edge(1226329766,1554094361,338))

#Number of airports
graph.numVertices
res10: Long = 305

#number of unique routes
scala> graph.numEdges
res11: Long = 5366

#Top 10 most busiest routes based on no of flights
echo "val N = 10" | spark-shell
N: Int = 10

echo "graph.triplets.sortBy(_.attr,ascending=false).map(triplet => "There are " + triplet.attr.toString + " flights from " + triplet.srcAttr + " to " + triplet.dstAttr + ".").take(N)" | spark-shell
#res13: Array[String] = Array(There are 13788 flights from SFO to LAX., There are 13390 flights from LAX to SFO., There are 12383 flights from OGG to HNL., There are 12035 flights from LGA to BOS., There are 12029 flights from BOS to LGA., There are 12014 flights from HNL to OGG., There are 11773 flights from LAX to LAS., There are 11729 flights from LAS to LAX., There are 11257 flights from LAX to SAN., There are 11224 flights from SAN to LAX.)

#Top 10 least busiest routes based on no of flights 
echo "graph.triplets.sortBy(_.attr,ascending=true).map(triplet => "There are " + triplet.attr.toString + " flights from " + triplet.srcAttr + " to " + triplet.dstAttr + ".").take(N)" | spark-shell
#res14: Array[String] = Array(There are 1 flights from SNA to SAN., There are 1 flights from ATW to DSM., There are 1 flights from AMA to TUL., There are 1 flights from ERI to PIT., There are 1 flights from COS to CLE., There are 1 flights from COS to FAT., There are 1 flights from COS to LAN., There are 1 flights from PLN to DCA., There are 1 flights from IND to PIT., There are 1 flights from CDC to SGU.)

#10 most popular airports based on no of routes serviced (most no of edges inputted into the vertex)
echo "graph.inDegrees.sortBy(_._2, ascending=false).take(10)"
#res18: Array[(org.apache.spark.graphx.VertexId, Int)] = Array((1710100676,173), (-520037749,148), (813811672,134), (-1895512637,126), (-1020692585,122), (178004914,118), (-1954981156,113), (-297351069,112), (1226329766,111), (1917476200,91))

#Most popular airport (top 1 most no of routes/no of edges into the vertex)
echo "graph.inDegrees.sortBy(_._2, ascending=false).take(1)"
#res16: Array[(org.apache.spark.graphx.VertexId, Int)] = Array((1710100676,173))

#Get the Airport name of the most popular airport
#Joining degrees with airportVertices gives Array(key, (degree, name))
#Sort by the degree (2nd element, first item) from greatest to least, take the top vertex with greatest degree and map it to the aiport name (2nd element, 2nditem)
echo "graph.inDegrees.join(airportVertices).sortBy(_._2._1,ascending=false).take(1).map(_._2._2)"
#res17: Array[String] = Array(ATL)
#Most popular airport is Atlanta

#Least popular airport (top 1 least no of routes/no of edges into vertex)
echo "graph.inDegrees.sortBy(_._2, ascending=true).take(1)"
#res19: Array[(org.apache.spark.graphx.VertexId, Int)] = Array((1290932502,1))

echo "graph.inDegrees.join(airportVertices).sortBy(_._2._1,ascending=true).take(1).map(_._2._2)"
#res21: Array[String] = Array(IYK)
#least popular airport in terms of number of routes terminating is IYK

#Most popular origin (based on no of edges going out of the vertex)
echo "graph.outDegrees.join(airportVertices).sortBy(_._2._1,ascending=false).take(1).map(_._2._2)"
res22: Array[String] = Array(ATL)
#Most popular origin is also Atlanta

#Ranking the airports based on the PageRank Algorithm (Airports which are most sought after destination based on no if input nodes and rank of those input nodes and other connections for those input nodes - the less connections the higher the contribution from the input node)

#set terminating criteria
scala> val stopAt = 0.0001
stopAt: Double = 1.0E-4

#calc ranks for the vertices (airports) using pageRank library
scala> val ranks = graph.pageRank(stopAt).vertices
ranks: org.apache.spark.graphx.VertexRDD[Double] = VertexRDDImpl[938] at RDD at VertexRDD.scala:57

#join the ranks with the vertices to get the Array[vertexId, (rank, name)]
scala> ranks.join(airportVertices).sortBy(_._2._1, ascending=false).map(_._2._2).take(10)
res23: Array[String] = Array(ATL, DFW, ORD, MSP, SLC, DEN, DTW, IAH, CVG, LAX)

scala> ranks.join(airportVertices).sortBy(_._2._1, ascending=false).map(_._2._1).take(10)
res24: Array[Double] = Array(10.959088881669425, 7.900006568344801, 7.705733234273574, 7.513932452834967, 7.286100755225603, 6.640310894326414, 6.240872965937828, 6.008826072669601, 5.602434800536124, 4.580629622323393)





