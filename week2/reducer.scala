#!/usr/bin/env scala
import scala.io.Source

var wordCount = scala.collection.immutable.Map[String,Int]()
for (ln <- Source.stdin.getLines) {
   var wordOne = ln.split("\t")
   if (wordCount.contains(wordOne(0))) {
      wordCount += wordOne(0) -> (wordCount(wordOne(0)) + wordOne(1).toInt)
   } else {
   wordCount += (wordOne(0) -> wordOne(1).toInt)
   }
}
 
