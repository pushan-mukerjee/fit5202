!#/bin/bash

val links = sc.textFile("urlLinks.txt").map(line => (line.split(" ")(0), line.split(" ")(1))).map(x => (x._1, x._2.split(","))).persist()

links.collect()
#Result is RDD consisting of an array of Key-Values pairs. 
#The Key represents a page and Values are an array of connected outlinks 
#res11: Array[(String, Array[String])] = Array((urlF,Array(urlB, urlA)), (urlG,Array(urlA, urlC)), (urlH,Array(urlA, urlD)), (urlB,Array(urlG)), (urlA,Array(urlC)), (urlC,Array(urlZ)), (urlD,Array(urlG, urlZ)))

#Setting initial Ranks. N=7, rank = 1/N. 1/7 gives 0 so must cast N to float
val N = 7
var ranks = links.map(x => (x._1, (1/N.toFloat))) 
ranks.collect()
#Result is an RDD consisting of an Array of key-value tuples where value is initial rank 0.14285715 or 1/7
#res10: Array[(String, Float)] = Array((urlF,0.14285715), (urlG,0.14285715), (urlH,0.14285715), (urlB,0.14285715), (urlA,0.14285715), (urlC,0.14285715), (urlD,0.14285715))


#Setting Array of (url, contribution) pairs
val contribs = links.join(ranks).flatMap{case (url, (links,rank)) => links.map(dest => (dest, rank/links.size))}

#Result is an RDD of (outlink, contribution) tuples
#res21: Array[(String, Float)] = Array((urlC,0.14285715), (urlZ,0.14285715), (urlA,0.071428575), (urlC,0.071428575), (urlB,0.071428575), (urlA,0.071428575), (urlG,0.14285715), (urlA,0.071428575), (urlD,0.071428575), (urlG,0.071428575), (urlZ,0.071428575))

#Recalc Ranks by Summing up the contributions by outlink URL key and calculate the page rank per URL using th [a/N + (1-a)*sum] formula
ranks = contribs.reduceByKey(_+_).mapValues(sum => alpha.toFloat/N.toFloat + (1-alpha.toFloat)*sum)

#Result is RDD of urls and their ranks 
#res23: Array[(String, Float)] = Array((urlA,0.15357143), (urlC,0.15357143), (urlG,0.15357143), (urlB,0.13214286), (urlZ,0.15357143), (urlD,0.13214286))

#Put it in a for loop to make it iteratively update the ranks up to a maxIter of 100

#Set max Iterations or termination criteria
val maxIter = 100

#Update Ranks
for (i <- 1 to maxIter) {
     val contribs = links.join(ranks).flatMap{case (url, (links,rank)) => links.map(dest => (dest, rank/links.size))}
     ranks = contribs.reduceByKey(_+_).mapValues(sum => alpha.toFloat/N.toFloat + (1-alpha.toFloat)*sum)
}

#Return the results and view the ranks
ranks.collect()

#Ranks
scala> ranks.collect()
res43: Array[(String, Float)] = Array((urlF,0.14285715), (urlG,0.14285715), (urlH,0.14285715), (urlB,0.14285715), (urlA,0.14285715), (urlC,0.14285715), (urlD,0.14285715))

