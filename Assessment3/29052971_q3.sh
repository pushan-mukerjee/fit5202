#!/bin/bash

echo "Pushan Mukerjee"
echo "29052971"

echo "

/* adjusting the log level to only errors */
sc.setLogLevel(\"error\")

/* 1. Create a Spark Dataframe and then show its contents */

val df = spark.read.csv(\"./dataset/iris.csv\")

df.show()

/* 2. Print the data frame's schema */

df.printSchema()

/* 3. Convert the dataframe to an RDD and display its contents */

/* convert df to RDD */
val myRDD = df.rdd

/*display contents of myRDD */
myRDD.collect()

/* 4. Create an RDD by reading ./dataset/big.txt and verify its contents by reading the first 5 lines */

/* Read in the big.txt */
val lines = sc.textFile(\"./dataset/big.txt\")

/* print the first 5 lines */

lines.take(5)

/* 5. Count the number of chars including white space in the text file using map and reduce functions */

val lineLengths = lines.flatMap(line => line.split(\"\").map(char => (char,1))).reduceByKey(_+_).count

println(\"end of Q3\")
/* end! */

" | spark-shell 
