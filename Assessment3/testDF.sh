#!/bin/bash

echo "

val df = spark.read.csv(\"iris.csv\")

df.show()

df.count()

df.show(1)

df.show(10)

df.describe()
df.columns()

df.select(\"sepal_length\").collect().map(_(0)).toList

df.printSchema()

df.foreach(row => println(row.get(2)))

df.filter($\"_c0\" < 5).map(row => println(row.get(0)))

" | spark-shell
