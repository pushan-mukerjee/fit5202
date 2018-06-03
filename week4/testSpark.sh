#!/bin/bash

echo "Student Name: Pushan Mukerjee"
echo "Student Number: 29052971"

echo "val smallText = sc.textFile(\"big.txt\")" | spark-shell

echo "def time[R](block: => R): R = {
          val t0 = System.nanoTime()
          val result = block    // call-by-name
          val t1 = System.nanoTime()
          println(\"Elapsed time: \" + (t1 - t0) + \"ns\")
          result
      }" | spark-shell 

echo "val wc = bigText.flatMap(line => line.split(\" \")).map(word => (word,1)).reduceByKey((a, b) => a + b)" | spark-shell

#count how many words in the document
echo "time{wc.count}" | spark-shell

#cache the document
echo "time{wc.cache}" | spark-shell

#count how many words in the document
echo "time{wc.count}" | spark-shell

#count only words between 3 and 7 letters - filter word.length
echo "val wc = bigText.flatMap(line => line.split(" ")).filter(word => word.length >= 3 && word.length <=7).map(word => (word,1)).reduceByKey((a, b) => a + b)" | spark-shell

#word count for words that start in a or b but not end in c or d - filter regexp
echo "val wc = bigText.flatMap(line => line.split(" ")).filter(word => word.matches("\b[a]w+ | \b[b]w+") && word.matches("w+[^c]\b | w+[^d]\b")).map(word => (word,1)).reduceByKey((a, b) => a + b)" | spark-shell

#word count for words that contain Alt - filter contains 
echo "val wc = smallText.flatMap(line => line.split(" ")).filter(word => word.contains("Alt")).map(word => (word,1)).reduceByKey((a, b) => a + b)" | spark-shell

#counting the number of lines in a file
echo "val linecnt = smallText.count" | spark-shell

#length of each line in the file in terms of characters
echo "val lineLen = smallText.map(line => line.length).collect()" | spark-shell

#collecting the number of words per line
echo "val WordCountPerLine = smallText.map(line => line.split(" ")).map(word => word.length).collect()" | spark-shell
#wc: Array[Int] = Array(5, 4, 5, 5, 5, 7, 7, 7, 7, 7, 7, 7, 1)

#Max and min number of words in a line - min, max
echo "val WordCountPerLine = smallText.map(line => line.split(" ")).map(word => word.length).max()" | spark-shell

#Max and min number of words in a line - min, max
echo "val WordCountPerLine = smallText.map(line => line.split(" ")).map(word => word.length).min()" | spark-shell

#counting the number of words in the file - flatMap converts lines to array of words 
val wc = smallText.flatMap(line => line.split(" ")).count

#collecting the nunber of chars per word (ie. word length) - flatMap, count
val wc = smallText.flatMap(line => line.split(" ")).map(word => word.length).collect()

#longest and shortest word in the file
echo "val wc = smallText.flatMap(line => line.split(" ")).map(word => word.length).max()" | spark-shell
echo "val wc = smallText.flatMap(line => line.split(" ")).map(word => word.length).min()" | spark-shell

#counting the number of words in each line - map line.split.length
echo "val wc = smallText.map(line => line.split(" ").length).collect()" | spark-shell
echo "val wc = smallText.map(line => line.split(" ").length).max" | spark-shell
echo "val wc = smallText.map(line => line.split(" ").length).min" | spark-shell

#Add the number of words in each line to get the total number of words in file - accumalator
 
