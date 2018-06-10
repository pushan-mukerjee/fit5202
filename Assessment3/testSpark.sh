#!/bin/bash
echo "Student Name: Pushan Mukerjee"
echo "Student Number: 29052971"

echo "

/* creating the bigText file from source */

val bigText = sc.textFile(\"big.txt\")

/* getting word count */
/* splits lines on white space to get words */
/* replaces punctuation with blank, trims whitespace, converts lower-case */
/* filters out empty words and only matches alphanumeric characters */

val wcount = bigText.flatMap(line => line.split(\"\\\W+\")).map(_.replaceAll(\"[{~,!,@,#,$,%,^,&,*,(,),_,=,-,\`,:,',?,/,<,>,.}]\", \"\").trim.toLowerCase).filter(word => !word.isEmpty && word.matches(\"[A-Za-z]+\")).map(word => (word,1)).reduceByKey(_+_)

/* determines 10 most frequent words */

wcount.sortBy(_._2, ascending=false).take(10)

/* Sorting words by Alphabetical order and listing first 30 */

wcount.sortByKey().take(30)

/* Sorting by reverse alphabetical order and listing first 30 */

wcount.sortByKey(false).take(30) 

/* Filtering Stopwords */

val stopWords = Set(\"the\", \"of\", \"and\", \"to\", \"in\", \"a\", \"he\", \"that\", \"was\", \"his\")

val wcountNoStop = bigText.flatMap(line => line.split(\"\\\s\")).map(_.replaceAll(\"[{~,!,@,#,$,%,^,&,*,(,),_,=,-,\`,:,',?,/,<,>,.}]\", \"\").trim.toLowerCase).filter(word => !word.isEmpty && word.matches(\"[A-Za-z]+\")).filter(word => !stopWords.contains(word)).map(word => (word,1)).reduceByKey(_+_)

/* 10 most frequent words after filtering stopwords */

wcountNoStop.sortBy(_._2, ascending=false).take(10)

/* Total Word Count */

/* initialises accumulator */
val accum = sc.longAccumulator(\"myAccum\")

/* transforms word cnt into an array of counts */

val wordCountArray = wcountNoStop.map(wordcnt => wordcnt._2).collect.toArray

/* adds each word count in the array to the accumulator */

val totalWordCount = wordCountArray.foreach(count => accum.add(count)) 

/* Returns total words which value of the accumulator */

accum.value

/* shorter form of word count with stopwords */

val word_cnt = bigText.flatMap(line => line.split(\"\\\W+\")).map(_.replaceAll(\"[{~,!,@,#,$,%,^,&,*,(,),_,=,-,\`,:,',?,/,<,>,.}]\", \"\").trim.toLowerCase).filter(word => !word.isEmpty && word.matches(\"[A-Za-z]+\")).map(word => (word,1)).count

/* shorter form of word count without stopwords */

val wcountNoStop = bigText.flatMap(line => line.split(\"\\\W+\")).map(_.replaceAll(\"[{~,!,@,#,$,%,^,&,*,(,),_,=,-,\`,:,',?,/,<,>,.}]\", \"\").trim.toLowerCase).filter(word => !word.isEmpty && word.matches(\"[A-Za-z]+\")).filter(word => !stopWords.contains(word)).map(word => (word,1)).count

/* length of the longest word */

/* Using wcountNoStop RDD (key, wordcnt) after reduceByKey */

val wcountNoStop = bigText.flatMap(line => line.split(\"\\\W+\")).map(_.replaceAll(\"[{~,!,@,#,$,%,^,&,*,(,),_,=,-,\`,:,',?,/,<,>,.}]\", \"\").trim.toLowerCase).filter(word => !word.isEmpty && word.matches(\"[A-Za-z]+\")).filter(word => !stopWords.contains(word)).map(word => (word,1)).reduceByKey(_+_).map(_._1.length).max

/* val maxWordLength = wcountNoStop.map(x => x._1.length).max */

val maxWordLength = bigText.flatMap(line => line.split(\"\\\W+\")).map(_.replaceAll(\"[{~,!,@,#,$,%,^,&,*,(,),_,=,-,\`,:,',?,/,<,>,.}]\", \"\").trim.toLowerCase).filter(word => !word.isEmpty && word.matches(\"[A-Za-z]+\")).filter(word => !stopWords.contains(word)).map(_.length).max

/* length of the shortest word */

val wcountNoStop = bigText.flatMap(line => line.split(\"\\\s\")).map(_.replaceAll(\"[{~,!,@,#,$,%,^,&,*,(,),_,=,-,\`,:,',?,/,<,>,.}]\", \"\").trim.toLowerCase).filter(word => !word.isEmpty && word.matches(\"[A-Za-z]+\")).filter(word => !stopWords.contains(word)).map(_.length).min

/* Number of partitions of big.txt */

val numPartitions = bigText.getNumPartitions

/* Change number of partitions */
val numPartitions = bigText.repartition(5).getNumPartitions 

/* How many lines or items stored in big text */ 

val numLines = bigText.count

/* Print the Lines/Items */

bigText.collect

/* Print the first 10 lines */

bigText.take(10)

/* Count the number of words */

val numWords = bigText.flatMap(line => line.split(\"\\\W+\")).map(_.replaceAll(\"[{~,!,@,#,$,%,^,&,*,(,),_,=,-,\`,:,',?,/,<,>,.}]\", \"\").trim.toLowerCase).filter(word => !word.isEmpty && word.matches(\"[A-Za-z]+\")).filter(word => !stopWords.contains(word)).count

/* Print the first word */

val firstWord = bigText.flatMap(line => line.split(\"\\\W+\")).map(_.replaceAll(\"[{~,!,@,#,$,%,^,&,*,(,),_,=,-,\`,:,',?,/,<,>,.}]\", \"\").trim.toLowerCase).filter(word => !word.isEmpty && word.matches(\"[A-Za-z]+\")).filter(word => !stopWords.contains(word)).first()

/* Print the first 10 words */

val first10 = bigText.flatMap(line => line.split(\"\\\W+\")).map(_.replaceAll(\"[{~,!,@,#,$,%,^,&,*,(,),_,=,-,\`,:,',?,/,<,>,.}]\", \"\").trim.toLowerCase).filter(word => !word.isEmpty && word.matches(\"[A-Za-z]+\")).filter(word => !stopWords.contains(word)).take(10)

/* Print 10 random samples */

val samples = bigText.flatMap(line => line.split(\"\\\W+\")).map(_.replaceAll(\"[{~,!,@,#,$,%,^,&,*,(,),_,=,-,\`,:,',?,/,<,>,.}]\", \"\").trim.toLowerCase).filter(word => !word.isEmpty && word.matches(\"[A-Za-z]+\")).filter(word => !stopWords.contains(word)).takeSample(false, 10).foreach(println)

/* Sample 50% of the words */

/* val samples = bigText.flatMap(line => line.split(\"\\\W+\")).map(_.replaceAll(\"[{~,!,@,#,$,%,^,&,*,(,),_,=,-,\`,:,',?,/,<,>,.}]\", \"\").trim.toLowerCase).filter(word => !word.isEmpty && word.matches(\"[A-Za-z]+\")).filter(word => !stopWords.contains(word)).sample(false, 0.5).foreach(println) */

/* How many lines in the file */

val numLines = bigText.map(line => line).count

/* how many lines have an asterix */

val numAsterix = bigText.filter(line => line.contains(\"*\")).count 

/* how many lines have the text 'and' */

val firstWord = bigText.filter(line => line.contains(\"and\")).count

/* how many lines contain the distinct word "and" */

val numAnds = bigText.map(_.split(\"\\\W+\")).filter(_.contains(\"and\")).count

/* how many blank lines */

val numBlankLines = bigText.filter(_.length == 0).count

/* get the lines which are not blank */
val nonBlankLines = bigText.filter(_.length != 0).map(line => line.split(\"\\\W+\")).collect

/* get words from the textfile and skip the blank lines */
val wordsSkipBlankLines = bigText.filter(_.length != 0).flatMap(line => line.split(\"\\\W+\")).take(10)


/* Length of longest line ie. number of words of longest line */

val longestLine = bigText.map(_.split(\" \").length).max

/* Frequency of each word */

val wordFreq = bigText.flatMap(line => line.split(\"\\\W+\")).map(_.replaceAll(\"[{~,!,@,#,$,%,^,&,*,(,),_,=,-,\`,:,',?,/,<,>,.}]\", \"\").trim.toLowerCase).filter(word => !word.isEmpty && word.matches(\"[A-Za-z]+\")).filter(word => !stopWords.contains(word)).map(word => (word,1)).reduceByKey(_+_).collect

/* Frequency of each word sorted alphabetically */

val wordFreq = bigText.flatMap(line => line.split(\"\\\W+\")).map(_.replaceAll(\"[{~,!,@,#,$,%,^,&,*,(,),_,=,-,\`,:,',?,/,<,>,.}]\", \"\").trim.toLowerCase).filter(word => !word.isEmpty && word.matches(\"[A-Za-z]+\")).filter(word => !stopWords.contains(word)).map(word => (word,1)).reduceByKey(_+_).sortByKey().collect 

/* Frequency of each word sorted by greatest freq to least freq, print top 10 */

val wordFreq = bigText.flatMap(line => line.split(\"\\\W+\")).map(_.replaceAll(\"[{~,!,@,#,$,%,^,&,*,(,),_,=,-,\`,:,',?,/,<,>,.}]\", \"\").trim.toLowerCase).filter(word => !word.isEmpty && word.matches(\"[A-Za-z]+\")).filter(word => !stopWords.contains(word)).map(word => (word,1)).reduceByKey(_+_).sortBy(_._2,ascending=false).take(10).foreach(println)

/* Longest word */

val wordLength = bigText.flatMap(line => line.split(\"\\\W+\")).map(_.replaceAll(\"[{~,!,@,#,$,%,^,&,*,(,),_,=,-,\`,:,',?,/,<,>,.}]\", \"\").trim.toLowerCase).filter(word => !word.isEmpty && word.matches(\"[A-Za-z]+\")).filter(word => !stopWords.contains(word)).map(word => (word, word.length)).groupBy(_._2).max

/* length of each word, sorted alphabetically */

val wordLength = bigText.flatMap(line => line.split(\"\\\W+\")).map(_.replaceAll(\"[{~,!,@,#,$,%,^,&,*,(,),_,=,-,\`,:,',?,/,<,>,.}]\", \"\").trim.toLowerCase).filter(word => !word.isEmpty && word.matches(\"[A-Za-z]+\")).filter(word => !stopWords.contains(word)).map(word => (word,word.length)).sortBy(_._1).take(10)


/* length of each word, sorted by length desc, top 10 longest words*/

val wordLength = bigText.flatMap(line => line.split(\"\\\W+\")).map(_.replaceAll(\"[{~,!,@,#,$,%,^,&,*,(,),_,=,-,\`,:,',?,/,<,>,.}]\", \"\").trim.toLowerCase).filter(word => !word.isEmpty && word.matches(\"[A-Za-z]+\")).filter(word => !stopWords.contains(word)).map(word => (word,word.length)).sortBy(_._2,ascending=false).take(10)

/* Number of unique words */

val numUniqueWords = bigText.flatMap(line => line.split(\"\\\W+\")).map(_.replaceAll(\"[{~,!,@,#,$,%,^,&,*,(,),_,=,-,\`,:,',?,/,<,>,.}]\", \"\").trim.toLowerCase).filter(word => !word.isEmpty && word.matches(\"[A-Za-z]+\")).filter(word => !stopWords.contains(word)).distinct.count

/* Filter out words starting with 'a' */

val numUniqueWords = bigText.flatMap(line => line.split(\"\\\W+\")).map(_.replaceAll(\"[{~,!,@,#,$,%,^,&,*,(,),_,=,-,\`,:,',?,/,<,>,.}]\", \"\").trim.toLowerCase).filter(word => !word.isEmpty && word.matches(\"[A-Za-z]+\")).filter(word => !stopWords.contains(word)).filter(word => !word.matches(\"^a(.*)$\")).map(word => (word,1)).reduceByKey(_+_).sortBy(_._1).take(10)

/* Find all words with length <=4 and add to stopwords */

val stopWords = bigText.flatMap(line => line.split(\"\\\W+\")).map(_.replaceAll(\"[{~,!,@,#,$,%,^,&,*,(,),_,=,-,\`,:,',?,/,<,>,.}]\", \"\").trim.toLowerCase).filter(word => !word.isEmpty && word.matches(\"[A-Za-z]+\")).filter(word => word.length <4).distinct.collect.toSet

/* val broadcastStopWords = sc.broadcast(stopWords.collect.toSet) */

stopWords.take(20)

/* Filter out the stopwords */

val wcountNoStop = bigText.flatMap(line => line.split(\"\\\W+\")).map(_.replaceAll(\"[{~,!,@,#,$,%,^,&,*,(,),_,=,-,\`,:,',?,/,<,>,.}]\", \"\").trim.toLowerCase).filter(word => !word.isEmpty && word.matches(\"[A-Za-z]+\")).filter(word => !stopWords.contains(word)).map(word => (word,1)).reduceByKey(_+_).collect()

/* Search how many times the word 'adventure' appears */ 

val wordFreqSearch = bigText.flatMap(line => line.split(\"\\\W+\")).map(_.replaceAll(\"[{~,!,@,#,$,%,^,&,*,(,),_,=,-,\`,:,',?,/,<,>,.}]\", \"\").trim.toLowerCase).filter(word => !word.isEmpty && word.matches(\"[A-Za-z]+\")).filter(word => !stopWords.contains(word)).map(word => (word,1)).reduceByKey(_+_).filter(_._1 == \"adventure\").collect


" | spark-shell
