#!/bin/bash

echo "Student Name: Pushan Mukerjee"
echo "Student Numner: 29052971"

BEGINCOMMENT
cala> val smallText = sc.textFile("smallText.txt").foreach(println)
[Stage 0:>                                                          (0 + 0) / 2]I. A Scandal in Bohemia
II. The Red-Headed League
III. A Case of Identity
IV. The Boscombe Valley Mystery
V. The Five Orange Pips
VI. The Man with the Twisted Lip
VII. The Adventure of the Blue Carbuncle
VIII. The Adventure of the Speckled Band
IX. The Adventure of the Engineer's Thumb
X. The Adventure of the Noble Bachelor
XI. The Adventure of the Beryl Coronet
XII. The Adventure of the Copper Beeches
<ctrl+d>
smallText: Unit = ()                                                            

scala> val smallText = sc.textFile("smallText.txt").getNumPartitions()
<console>:24: error: Int does not take parameters
       val smallText = sc.textFile("smallText.txt").getNumPartitions()
                                                                    ^

scala> val smallText = sc.textFile("smallText.txt").getNumPartitions
smallText: Int = 2

scala> val smallText = sc.textFile("smallText.txt").partitions.foreach(println)
org.apache.spark.rdd.HadoopPartition@43d
org.apache.spark.rdd.HadoopPartition@43e
smallText: Unit = ()

scala> val newRDD = smallText.repartition(5)
<console>:26: error: value repartition is not a member of Unit
       val newRDD = smallText.repartition(5)
                              ^

scala> smallText.getNumParitions()
<console>:27: error: value getNumParitions is not a member of Unit
       smallText.getNumParitions()
                 ^

scala> smallText.first()
<console>:27: error: value first is not a member of Unit
       smallText.first()
                 ^

scala> smallText.first
<console>:27: error: value first is not a member of Unit
       smallText.first
                 ^

scala> smallText.getNumParitions()
<console>:27: error: value getNumParitions is not a member of Unit
       smallText.getNumParitions()
                 ^

scala> smallText.getNumParitions
<console>:27: error: value getNumParitions is not a member of Unit
       smallText.getNumParitions
                 ^

scala> smallText.getNumParitions()
<console>:27: error: value getNumParitions is not a member of Unit
       smallText.getNumParitions()
                 ^

scala> smallText.getNumParitions
<console>:27: error: value getNumParitions is not a member of Unit
       smallText.getNumParitions
                 ^

scala> val smallText = textFile("smallText.txt")
<console>:23: error: not found: value textFile
       val smallText = textFile("smallText.txt")
                       ^

scala> val smallText = sc.textFile("smallText.txt")
smallText: org.apache.spark.rdd.RDD[String] = smallText.txt MapPartitionsRDD[7] at textFile at <console>:24

scala> smallText.getNumPartitions
res7: Int = 2

scala> smallText.repartition(5)
res8: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[11] at repartition at <console>:27

scala> smallText.partitions.foreach(println)
org.apache.spark.rdd.HadoopPartition@47b
org.apache.spark.rdd.HadoopPartition@47c

scala> smallText.partitions.getNumParitions
<console>:27: error: value getNumParitions is not a member of Array[org.apache.spark.Partition]
       smallText.partitions.getNumParitions
                            ^

scala> smallText.partitions.getNumParitions()
<console>:27: error: value getNumParitions is not a member of Array[org.apache.spark.Partition]
       smallText.partitions.getNumParitions()
                            ^

scala> smallText.getNumParitions()
<console>:27: error: value getNumParitions is not a member of org.apache.spark.rdd.RDD[String]
       smallText.getNumParitions()
                 ^

scala> smallText.getNumParitions
<console>:27: error: value getNumParitions is not a member of org.apache.spark.rdd.RDD[String]
       smallText.getNumParitions
                 ^

scala> smallText.getNumPartitions
res14: Int = 2

scala> val newRDD = smallText.repartition(5)
newRDD: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[15] at repartition at <console>:26

scala> newRDD.getNumPartitions()
<console>:29: error: Int does not take parameters
       newRDD.getNumPartitions()
                              ^

scala> newRDD.getNumPartitions
res16: Int = 5

scala> newRDD.partitions.foreach(println)
CoalescedRDDPartition(0,ShuffledRDD[13] at repartition at <console>:26,[I@d67f6f,None)
CoalescedRDDPartition(1,ShuffledRDD[13] at repartition at <console>:26,[I@4fc28b23,None)
CoalescedRDDPartition(2,ShuffledRDD[13] at repartition at <console>:26,[I@1b58788a,None)
CoalescedRDDPartition(3,ShuffledRDD[13] at repartition at <console>:26,[I@31881213,None)
CoalescedRDDPartition(4,ShuffledRDD[13] at repartition at <console>:26,[I@2162e4a,None)

scala> smallText.count
res18: Long = 13

scala> smallText.collect()
res19: Array[String] = Array(I. A Scandal in Bohemia, II. The Red-Headed League, III. A Case of Identity, IV. The Boscombe Valley Mystery, V. The Five Orange Pips, VI. The Man with the Twisted Lip, VII. The Adventure of the Blue Carbuncle, VIII. The Adventure of the Speckled Band, IX. The Adventure of the Engineer's Thumb, X. The Adventure of the Noble Bachelor, XI. The Adventure of the Beryl Coronet, XII. The Adventure of the Copper Beeches, <ctrl+d>)

scala> smallText.foreach(println)
I. A Scandal in Bohemia
II. The Red-Headed League
III. A Case of Identity
IV. The Boscombe Valley Mystery
V. The Five Orange Pips
VI. The Man with the Twisted Lip
VII. The Adventure of the Blue Carbuncle
VIII. The Adventure of the Speckled Band
IX. The Adventure of the Engineer's Thumb
X. The Adventure of the Noble Bachelor
XI. The Adventure of the Beryl Coronet
XII. The Adventure of the Copper Beeches
<ctrl+d>

scala> smallText.first()
res21: String = I. A Scandal in Bohemia

scala> smallText.take(3)
res22: Array[String] = Array(I. A Scandal in Bohemia, II. The Red-Headed League, III. A Case of Identity)

scala> smallText.takeSample(false, 3).foreach(println)
I. A Scandal in Bohemia
<ctrl+d>
IX. The Adventure of the Engineer's Thumb

scala> smallText.takeSample(true, 3).foreach(println)
VII. The Adventure of the Blue Carbuncle
IV. The Boscombe Valley Mystery
III. A Case of Identity

scala> smallText.takeSample(true, 3).foreach(println)
VI. The Man with the Twisted Lip
V. The Five Orange Pips
VIII. The Adventure of the Speckled Band

scala> smallText.takeSample(false, 3).foreach(println)
VII. The Adventure of the Blue Carbuncle
IV. The Boscombe Valley Mystery
IX. The Adventure of the Engineer's Thumb

scala> smallText.sample(false, 0.5).count()
res27: Long = 8

scala> smallText.sample(false, 0.5).count()
res28: Long = 6

scala> smallText.sample(false, 0.5).count()
res29: Long = 4

scala> smallText.sample(false, 0.8).count()
res30: Long = 12

scala> smallText.sample(false, 0.8).count()
res31: Long = 7

scala> smallText.sample(false, 0.8).count()
res32: Long = 10

scala> smallText.takeSample(false, 4).count()
<console>:27: error: not enough arguments for method count: (p: String => Boolean)Int.
Unspecified value parameter p.
       smallText.takeSample(false, 4).count()
                                           ^

scala> smallText.takeSample(false, 4)
res34: Array[String] = Array(VI. The Man with the Twisted Lip, III. A Case of Identity, XII. The Adventure of the Copper Beeches, II. The Red-Headed League)

scala> smallText.takeSample(false, 4).split(".")(0).foreach(println)
<console>:27: error: value split is not a member of Array[String]
       smallText.takeSample(false, 4).split(".")(0).foreach(println)
                                      ^

scala> smallText.takeSample(false, 4).map(_.split(".")(0).foreach(println))
java.lang.ArrayIndexOutOfBoundsException: 0
  at $anonfun$1.apply(<console>:27)
  at $anonfun$1.apply(<console>:27)
  at scala.collection.TraversableLike$$anonfun$map$1.apply(TraversableLike.scala:234)
  at scala.collection.TraversableLike$$anonfun$map$1.apply(TraversableLike.scala:234)
  at scala.collection.IndexedSeqOptimized$class.foreach(IndexedSeqOptimized.scala:33)
  at scala.collection.mutable.ArrayOps$ofRef.foreach(ArrayOps.scala:186)
  at scala.collection.TraversableLike$class.map(TraversableLike.scala:234)
  at scala.collection.mutable.ArrayOps$ofRef.map(ArrayOps.scala:186)
  ... 48 elided

scala> smallText.takeSample(false, 4).map(x => x.split(".")(0).foreach(println))
java.lang.ArrayIndexOutOfBoundsException: 0
  at $anonfun$1.apply(<console>:27)
  at $anonfun$1.apply(<console>:27)
  at scala.collection.TraversableLike$$anonfun$map$1.apply(TraversableLike.scala:234)
  at scala.collection.TraversableLike$$anonfun$map$1.apply(TraversableLike.scala:234)
  at scala.collection.IndexedSeqOptimized$class.foreach(IndexedSeqOptimized.scala:33)
  at scala.collection.mutable.ArrayOps$ofRef.foreach(ArrayOps.scala:186)
  at scala.collection.TraversableLike$class.map(TraversableLike.scala:234)
  at scala.collection.mutable.ArrayOps$ofRef.map(ArrayOps.scala:186)
  ... 48 elided

scala> smallText.takeSample(false, 4).foreach(println)
II. The Red-Headed League
VI. The Man with the Twisted Lip
IX. The Adventure of the Engineer's Thumb
VII. The Adventure of the Blue Carbuncle

scala> smallText.takeSample(false, 5).foreach(println)
X. The Adventure of the Noble Bachelor
VII. The Adventure of the Blue Carbuncle
VI. The Man with the Twisted Lip
VIII. The Adventure of the Speckled Band
XII. The Adventure of the Copper Beeches

scala> smallText.sample(false, 0.6).foreach(println)
X. The Adventure of the Noble Bachelor
XI. The Adventure of the Beryl Coronet
XII. The Adventure of the Copper Beeches
I. A Scandal in Bohemia
IV. The Boscombe Valley Mystery
V. The Five Orange Pips
VI. The Man with the Twisted Lip
VIII. The Adventure of the Speckled Band

scala> smallText.sample(false, 1).foreach(println)
I. A Scandal in Bohemia
II. The Red-Headed League
III. A Case of Identity
IV. The Boscombe Valley Mystery
V. The Five Orange Pips
VI. The Man with the Twisted Lip
VII. The Adventure of the Blue Carbuncle
VIII. The Adventure of the Speckled Band
IX. The Adventure of the Engineer's Thumb
X. The Adventure of the Noble Bachelor
XI. The Adventure of the Beryl Coronet
XII. The Adventure of the Copper Beeches
<ctrl+d>

scala> smallText.sample(false, 1).foreach(println)
I. A Scandal in Bohemia
II. The Red-Headed League
III. A Case of Identity
IV. The Boscombe Valley Mystery
V. The Five Orange Pips
VI. The Man with the Twisted Lip
VII. The Adventure of the Blue Carbuncle
VIII. The Adventure of the Speckled Band
IX. The Adventure of the Engineer's Thumb
X. The Adventure of the Noble Bachelor
XI. The Adventure of the Beryl Coronet
XII. The Adventure of the Copper Beeches
<ctrl+d>

scala> smallText.sample(false, 1).foreach(println)
I. A Scandal in Bohemia
II. The Red-Headed League
III. A Case of Identity
IV. The Boscombe Valley Mystery
V. The Five Orange Pips
VI. The Man with the Twisted Lip
VII. The Adventure of the Blue Carbuncle
VIII. The Adventure of the Speckled Band
IX. The Adventure of the Engineer's Thumb
X. The Adventure of the Noble Bachelor
XI. The Adventure of the Beryl Coronet
XII. The Adventure of the Copper Beeches
<ctrl+d>

scala> smallText.sample(false, 1).foreach(println)
I. A Scandal in Bohemia
II. The Red-Headed League
III. A Case of Identity
IV. The Boscombe Valley Mystery
V. The Five Orange Pips
VI. The Man with the Twisted Lip
VII. The Adventure of the Blue Carbuncle
VIII. The Adventure of the Speckled Band
IX. The Adventure of the Engineer's Thumb
X. The Adventure of the Noble Bachelor
XI. The Adventure of the Beryl Coronet
XII. The Adventure of the Copper Beeches
<ctrl+d>

scala> smallText.sample(false, 1).foreach(println)
IX. The Adventure of the Engineer's Thumb
X. The Adventure of the Noble Bachelor
XI. The Adventure of the Beryl Coronet
XII. The Adventure of the Copper Beeches
<ctrl+d>
I. A Scandal in Bohemia
II. The Red-Headed League
III. A Case of Identity
IV. The Boscombe Valley Mystery
V. The Five Orange Pips
VI. The Man with the Twisted Lip
VII. The Adventure of the Blue Carbuncle
VIII. The Adventure of the Speckled Band

scala> smallText.sample(false, 1).foreach(println)
I. A Scandal in Bohemia
II. The Red-Headed League
III. A Case of Identity
IV. The Boscombe Valley Mystery
V. The Five Orange Pips
VI. The Man with the Twisted Lip
VII. The Adventure of the Blue Carbuncle
VIII. The Adventure of the Speckled Band
IX. The Adventure of the Engineer's Thumb
X. The Adventure of the Noble Bachelor
XI. The Adventure of the Beryl Coronet
XII. The Adventure of the Copper Beeches
<ctrl+d>

scala> smallText.sample(false, 1).foreach(println)
IX. The Adventure of the Engineer's Thumb
X. The Adventure of the Noble Bachelor
XI. The Adventure of the Beryl Coronet
I. A Scandal in Bohemia
II. The Red-Headed League
III. A Case of Identity
IV. The Boscombe Valley Mystery
V. The Five Orange Pips
VI. The Man with the Twisted Lip
VII. The Adventure of the Blue Carbuncle
VIII. The Adventure of the Speckled Band
XII. The Adventure of the Copper Beeches
<ctrl+d>

scala> smallText.sample(false, 1).foreach(println)
IX. The Adventure of the Engineer's Thumb
X. The Adventure of the Noble Bachelor
XI. The Adventure of the Beryl Coronet
XII. The Adventure of the Copper Beeches
<ctrl+d>
I. A Scandal in Bohemia
II. The Red-Headed League
III. A Case of Identity
IV. The Boscombe Valley Mystery
V. The Five Orange Pips
VI. The Man with the Twisted Lip
VII. The Adventure of the Blue Carbuncle
VIII. The Adventure of the Speckled Band

scala> smallText.sample(false, 1).foreach(println)
IX. The Adventure of the Engineer's Thumb
X. The Adventure of the Noble Bachelor
XI. The Adventure of the Beryl Coronet
XII. The Adventure of the Copper Beeches
<ctrl+d>
I. A Scandal in Bohemia
II. The Red-Headed League
III. A Case of Identity
IV. The Boscombe Valley Mystery
V. The Five Orange Pips
VI. The Man with the Twisted Lip
VII. The Adventure of the Blue Carbuncle
VIII. The Adventure of the Speckled Band

scala> smallText.map(x => x.contains("of").count()
     | )
<console>:27: error: value count is not a member of Boolean
       smallText.map(x => x.contains("of").count()
                                           ^

scala> smallText.filter.map(x => x.contains("of")).foreach(println)
<console>:27: error: missing argument list for method filter in class RDD
Unapplied methods are only converted to functions when a function type is expected.
You can make this conversion explicit by writing `filter _` or `filter(_)` instead of `filter`.
       smallText.filter.map(x => x.contains("of")).foreach(println)
                 ^

scala> smallText.filter(x => x.contains("of")).foreach(println)
III. A Case of Identity
VII. The Adventure of the Blue Carbuncle
VIII. The Adventure of the Speckled Band
IX. The Adventure of the Engineer's Thumb
X. The Adventure of the Noble Bachelor
XI. The Adventure of the Beryl Coronet
XII. The Adventure of the Copper Beeches

scala> smallText.filter(_.contains("of")).foreach(println)
III. A Case of Identity
VII. The Adventure of the Blue Carbuncle
VIII. The Adventure of the Speckled Band
IX. The Adventure of the Engineer's Thumb
X. The Adventure of the Noble Bachelor
XI. The Adventure of the Beryl Coronet
XII. The Adventure of the Copper Beeches

scala> smallText.filter(_.!contains("of")).foreach(println)
<console>:27: error: value ! is not a member of String
       smallText.filter(_.!contains("of")).foreach(println)
                          ^

scala> smallText.filter(!_.contains("of")).foreach(println)
<ctrl+d>
I. A Scandal in Bohemia
II. The Red-Headed League
IV. The Boscombe Valley Mystery
V. The Five Orange Pips
VI. The Man with the Twisted Lip

scala> smallText.filter(!_.contains("of")).count()
res56: Long = 6

scala> smallText.filter(_.contains("of")).count()
res57: Long = 7

scala> smallText.count()
res58: Long = 13

scala> smallText.map(line => line.split(" ").length).max()
res59: Int = 7

scala> smallText.map(line => line.split(" ").length).max.println
<console>:27: error: value println is not a member of Int
       smallText.map(line => line.split(" ").length).max.println
                                                         ^

scala> smallText.map(line => line.split(" ").length).println(max())
<console>:27: error: value println is not a member of org.apache.spark.rdd.RDD[Int]
       smallText.map(line => line.split(" ").length).println(max())
                                                     ^
<console>:27: error: overloaded method value max with alternatives:
  (columnName: String)org.apache.spark.sql.Column <and>
  (e: org.apache.spark.sql.Column)org.apache.spark.sql.Column
 cannot be applied to ()
       smallText.map(line => line.split(" ").length).println(max())
                                                             ^

scala> smallText.map(line => line.split(" ").length).println(max)
<console>:27: error: value println is not a member of org.apache.spark.rdd.RDD[Int]
       smallText.map(line => line.split(" ").length).println(max)
                                                     ^

scala> smallText.map(line => line.split(" ").length).max
res63: Int = 7

scala> smallText.map(line => line.split(" ").length).max.println
<console>:27: error: value println is not a member of Int
       smallText.map(line => line.split(" ").length).max.println
                                                         ^

scala> smallText.map(line => line.split(" ").length).max.println()
<console>:27: error: value println is not a member of Int
       smallText.map(line => line.split(" ").length).max.println()
                                                         ^

scala> smallText.map(line => line.split(" ").length).count.foreach(println)
<console>:27: error: value foreach is not a member of Long
       smallText.map(line => line.split(" ").length).count.foreach(println)
                                                           ^

scala> smallText.map(line => line.split(" ").length).count
res67: Long = 13

scala> smallText.map(line => line.split(" ").length).min
res68: Int = 1

scala> smallText.map(line => line.split(" ").length).max
res69: Int = 7

scala> smallText.map(line => line.split(" ").length).println
<console>:27: error: value println is not a member of org.apache.spark.rdd.RDD[Int]
       smallText.map(line => line.split(" ").length).println
                                                     ^

scala> smallText.map(line => line.split(" ").length).print
<console>:27: error: value print is not a member of org.apache.spark.rdd.RDD[Int]
       smallText.map(line => line.split(" ").length).print
                                                     ^

scala> smallText.map(line => line.split(" ").length).print
<console>:27: error: value print is not a member of org.apache.spark.rdd.RDD[Int]
       smallText.map(line => line.split(" ").length).print
                                                     ^

scala> smallText.flatMap(line => line.split(" ").map(word => (word,1)).reduceByKey((a, b), (a + b)).collect()
     | 
     | smallText.flatMap(line => line.split(" ").map(word => (word,1)).reduceByKey((a, b), (a + b)).collect()
<console>:3: error: ')' expected but '.' found.
smallText.flatMap(line => line.split(" ").map(word => (word,1)).reduceByKey((a, b), (a + b)).collect()
         ^

scala> smallText.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey((a, b), (a + b)).collect()
<console>:27: error: not found: value a
       smallText.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey((a, b), (a + b)).collect()
                                                                                     ^
<console>:27: error: not found: value b
       smallText.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey((a, b), (a + b)).collect()
                                                                                        ^
<console>:27: error: not found: value a
       smallText.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey((a, b), (a + b)).collect()
                                                                                             ^
<console>:27: error: not found: value b
       smallText.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey((a, b), (a + b)).collect()
                                                                                                 ^

scala> smallText.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey((a, b) => (a + b)).collect()
res74: Array[(String, Int)] = Array((Case,1), (Pips,1), (XII.,1), (X.,1), (Coronet,1), (Bachelor,1), (with,1), (<ctrl+d>,1), (Thumb,1), (Blue,1), (Boscombe,1), (Man,1), (Scandal,1), (V.,1), (Beryl,1), (II.,1), (Orange,1), (Five,1), (Adventure,6), (Identity,1), (Twisted,1), (Noble,1), (VII.,1), (Carbuncle,1), (XI.,1), (The,10), (Red-Headed,1), (Beeches,1), (Lip,1), (VI.,1), (Valley,1), (Band,1), (IX.,1), (A,2), (League,1), (I.,1), (in,1), (Bohemia,1), (of,7), (Copper,1), (Mystery,1), (IV.,1), (VIII.,1), (Engineer's,1), (Speckled,1), (the,7), (III.,1))

scala> smallText.flatMap(_.split(" ")).map((_,1)).reduceByKey((_+_)).collect()
res75: Array[(String, Int)] = Array((Case,1), (Pips,1), (XII.,1), (X.,1), (Coronet,1), (Bachelor,1), (with,1), (<ctrl+d>,1), (Thumb,1), (Blue,1), (Boscombe,1), (Man,1), (Scandal,1), (V.,1), (Beryl,1), (II.,1), (Orange,1), (Five,1), (Adventure,6), (Identity,1), (Twisted,1), (Noble,1), (VII.,1), (Carbuncle,1), (XI.,1), (The,10), (Red-Headed,1), (Beeches,1), (Lip,1), (VI.,1), (Valley,1), (Band,1), (IX.,1), (A,2), (League,1), (I.,1), (in,1), (Bohemia,1), (of,7), (Copper,1), (Mystery,1), (IV.,1), (VIII.,1), (Engineer's,1), (Speckled,1), (the,7), (III.,1))

scala> val wc = smallText.flatMap(_.split(" ")).map((_,1)).reduceByKey((_+_)).collect()
wc: Array[(String, Int)] = Array((Case,1), (Pips,1), (XII.,1), (X.,1), (Coronet,1), (Bachelor,1), (with,1), (<ctrl+d>,1), (Thumb,1), (Blue,1), (Boscombe,1), (Man,1), (Scandal,1), (V.,1), (Beryl,1), (II.,1), (Orange,1), (Five,1), (Adventure,6), (Identity,1), (Twisted,1), (Noble,1), (VII.,1), (Carbuncle,1), (XI.,1), (The,10), (Red-Headed,1), (Beeches,1), (Lip,1), (VI.,1), (Valley,1), (Band,1), (IX.,1), (A,2), (League,1), (I.,1), (in,1), (Bohemia,1), (of,7), (Copper,1), (Mystery,1), (IV.,1), (VIII.,1), (Engineer's,1), (Speckled,1), (the,7), (III.,1))

scala> wc.cache()
<console>:29: error: value cache is not a member of Array[(String, Int)]
       wc.cache()
          ^

scala> val wc = smallText.flatMap(_.split(" ")).map((_,1)).reduceByKey((_+_))
wc: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[62] at reduceByKey at <console>:26

scala> wc.collect()
res77: Array[(String, Int)] = Array((Case,1), (Pips,1), (XII.,1), (X.,1), (Coronet,1), (Bachelor,1), (with,1), (<ctrl+d>,1), (Thumb,1), (Blue,1), (Boscombe,1), (Man,1), (Scandal,1), (V.,1), (Beryl,1), (II.,1), (Orange,1), (Five,1), (Adventure,6), (Identity,1), (Twisted,1), (Noble,1), (VII.,1), (Carbuncle,1), (XI.,1), (The,10), (Red-Headed,1), (Beeches,1), (Lip,1), (VI.,1), (Valley,1), (Band,1), (IX.,1), (A,2), (League,1), (I.,1), (in,1), (Bohemia,1), (of,7), (Copper,1), (Mystery,1), (IV.,1), (VIII.,1), (Engineer's,1), (Speckled,1), (the,7), (III.,1))

scala> wc.collect()
res78: Array[(String, Int)] = Array((Case,1), (Pips,1), (XII.,1), (X.,1), (Coronet,1), (Bachelor,1), (with,1), (<ctrl+d>,1), (Thumb,1), (Blue,1), (Boscombe,1), (Man,1), (Scandal,1), (V.,1), (Beryl,1), (II.,1), (Orange,1), (Five,1), (Adventure,6), (Identity,1), (Twisted,1), (Noble,1), (VII.,1), (Carbuncle,1), (XI.,1), (The,10), (Red-Headed,1), (Beeches,1), (Lip,1), (VI.,1), (Valley,1), (Band,1), (IX.,1), (A,2), (League,1), (I.,1), (in,1), (Bohemia,1), (of,7), (Copper,1), (Mystery,1), (IV.,1), (VIII.,1), (Engineer's,1), (Speckled,1), (the,7), (III.,1))

scala> 
END COMMENT
