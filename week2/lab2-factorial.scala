#!/usr/bin/env scala
import scala.io.Source
import scala.math

//define input file
val inputFile = "csv_input.csv"

val lines = Source.fromFile(inputFile).getLines.toList.map(x => { 
  val temp = x.split(",")
  (temp(0).toDouble,temp(1).toDouble)
  }) 

//calculate sqrt of avg square of differences in input file
val mse = Math.sqrt((lines.map(x => Math.pow(x._1 - x._2, 2)).reduceLeft(_+_))/lines.length)
println(mse)
