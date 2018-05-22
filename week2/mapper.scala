#!/usr/bin/env scala
import scala.io.Source

val filename = "baa.txt"
for (line <- Source.stdin.getLines) {
   line.split(" ").foreach(x => println(s"$x\t1"))
}


//for (line <- io.source.stdin.getLines) {
//   line.split(" ").foreach(x => println(s"$x\t1"))
//} 
