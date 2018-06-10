#!/bin/bash

echo "Pushan Mukerjee"
echo "29052971"

echo "

/* start! */

/* adjusting the log level to only errors */
sc.setLogLevel(\"error\")

/* Create an RDD by reading ./dataset/big.txt */
val lines = sc.textFile(\"./dataset/big.txt\")

/* Start Q6 */
println(\"START Q6\")

/*
6. Find and display the top 100 words of the ./dataset/big.txt file with their frequency ordered from the most frequent to least 

The vocabulary should be:
  case-insensitive
  not contain tokens without alphabets eg. '711' or '!?' however 1st and 5pm are fine 
  not contain empty words or words contain any space in it eg. '' or '  ' 
 Notes:
   ignore '\"?!-_)({}[] symbols
   words can be tokenized with any number of whitespace characters
*/

/* Explanation of code: 
- splits on whitespace
- removes the following punctuation (seperated by comma): 
[,],{,},~,!,@,#,$,%,^,&,*,(,),_,=,-,\`,:,',?,/,<,>,.
- Applies filter condition: Remove empty words AND (include words that are alpha only case in-sensitive, OR begin with digit followed by alpha case insensitive eg. 1st or 2pm, 2PM etc)
- maps the words to a RDD of (word, 1) counting each word once
- reduces each word by the word key and sums their count to give a (word, frequency) RDD where each word is unique. 
- sorts by the frequency (2nd item in the tuple) with ascending = false (greatest to least frequency)
- prints out the first 100 which are the top 100 frequent words
*/

val hundredFrequentWords = lines.flatMap(line => line.split(\"\\\s+\")).map(_.replaceAll(\"[{[,],{,},~,!,@,#,$,%,^,&,*,(,),_,=,-,\`,:,',?,/,<,>,.}]\", \"\").trim).filter(word => !word.isEmpty && (word.matches(\"[A-Za-z]+\") | word.matches(\"[0-9][A-Za-z]\"))).map(word => (word,1)).reduceByKey(_+_).sortBy(_._2, ascending=false).take(100).foreach(println)

/* End Q6 */ 
println(\"END Q6\")

/* Start Q7 */
println(\"START Q7\")
/*
7. Write a program which does word count of the ./dataset/big.txt file using Spark (note words may be seperated by more than one whitespace)
- lowercase all letters
- filters out stopwords
- filters out the following punctuation (replaces with \"\"): 
[,],{,},~,!,@,#,$,%,^,&,*,(,),_,=,-,\`,:,',?,/,<,>,. 
- filters out digits (replaces with \"\")
- ignores quotation marks
- performs a word count of unique words 
- performs a total word count of all words by summing the frequency of each unique word
- prints 10 most popular (most frequent) words ordered by frequency in decsending order
- prints 10 least popular (least frequent) words ordered by frequency in ascending order
- Explanation of code is in each of the steps below
*/

/*Creating a Set for the stop words */
val stopWords = Set(\"a\", \"about\", \"above\", \"after\", \"again\", \"against\", \"all\", \"am\", \"an\", \"and\", \"any\", \"are\", \"as\", \"at\", \"be\", \"because\", \"been\", \"before\", \"being\", \"below\", \"between\", \"both\", \"but\", \"by\", \"could\", \"did\", \"do\", \"does\", \"doing\", \"down\", \"during\", \"each\", \"few\", \"for\", \"from\", \"further\", \"had\", \"has\", \"have\", \"having\", \"he\", \"her\", \"here\", \"hers\", \"herself\", \"him\", \"himself\", \"his\", \"how\", \"i\", \"if\", \"in\", \"into\", \"is\", \"it\", \"its\", \"itself\", \"me\", \"more\", \"most\", \"my\", \"myself\", \"nor\", \"of\", \"on\", \"once\", \"only\", \"or\", \"other\", \"ought\", \"our\", \"ours\", \"ourselves\", \"out\", \"over\", \"own\", \"same\", \"she\", \"should\", \"so\", \"some\", \"such\", \"than\", \"that\", \"the\", \"themselves\", \"then\", \"there\", \"these\", \"they\", \"this\", \"those\", \"through\", \"to\", \"too\", \"under\", \"until\", \"up\", \"very\", \"was\", \"we\", \"were\", \"what\", \"when\", \"where\", \"which\", \"while\", \"who\", \"whom\", \"why\", \"with\", \"would\", \"you\", \"your\", \"yours\", \"yourself\", \"yourselves\")

/* Wrangle the big.txt to get the unique words, using the above rules, in a (word, frequency) tuple 
- splits on whitespace
- filters out punctuation and digits and replaces them with blanks
- filters out empty words
- applies filter to match on alpha words only 
- Note: the 1pm is removed since digits are replaced with blanks
- filters out words which are in the stopwords set 
- maps a (word, 1) tuple with frequency of each word set to 1 
- reduces the (word, 1) tuple by word key to get (word, frequency) tuple 
*/
val words = lines.flatMap(line => line.split(\"\\\s+\")).map(_.replaceAll(\"[{[,],{,},~,!,@,#,$,%,^,&,*,(,),_,=,-,\`,:,',?,/,<,>,.,1,2,3,4,5,6,7,8,9,0}]\", \"\").trim.toLowerCase).filter(word => !word.isEmpty && word.matches(\"[A-Za-z]+\") ).filter(word => !stopWords.contains(word)).map(word => (word,1)).reduceByKey(_+_)

/* Print the number of unique words in the document */
val wordCount = words.count

/* 
Sum the frequency of each unique word to get the Total Word Count in document. 
3 steps described below:
*/

/* 
Step 1. Initialise accumulator to 0 which will store Total Word Count 
*/
val accum = sc.longAccumulator(\"myAccum\")

/* 
Step 2. From the (word, frequency) tuples, get total word count by putting each frequency into an array and summing the frequencies by adding them to the accumulator
*/
val totalWords = words.map(wordcnt => wordcnt._2).collect.toArray.foreach(count => accum.add(count))

/* 
Step 3. Print the Total Word Count which is final value of the accumulator
*/
accum.value

/* Print the 10 Most Popular words (most frequent) by using the (word, frequency) tuples and sorting by frequency in descending order (most frequent on top) 
*/
val TenMostPopularWords = words.sortBy(_._2, ascending=false).take(10).foreach(println)

/* Print the 10 Least Popular words (least frequent) by using the (word, frequency) tuples and sorting by frequency in ascending order (least frequent on top)
*/ 
val TenLeastPopularWords = words.sortBy(_._2, ascending=true).take(10).foreach(println)

/* END END */
println(\"END END\")

" | spark-shell

