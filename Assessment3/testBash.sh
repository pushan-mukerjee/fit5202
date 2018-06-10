#!/bin/bash

echo "

val bigText = sc.textFile(\"big.txt\")

val lines = bigText.flatMap(line => line.split(\"\\\s\"))

val words = lines.map(_.replaceAll(\"[{~,!,@,#,$,%,^,&,*,(,),_,=,-,\`,:,',?,/,<,>,.}]\", \"\").trim.toLowerCase)

val filteredWords = words.filter(word => !word.isEmpty && word.matches(\"[A-Za-z]+\")).map(word => (word,1)).reduceByKey(_+_)

" | spark-shell
