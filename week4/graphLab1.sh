scala> import org.apache.spark.graphx._
import org.apache.spark.graphx._

scala> import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD

scala> val users: RDD[(VertexId, (String, String))] =
     | sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")), (5L, ("franklin", "professor")), (2L, ("istoica", "professor"))))
users: org.apache.spark.rdd.RDD[(org.apache.spark.graphx.VertexId, (String, String))] = ParallelCollectionRDD[0] at parallelize at <console>:29

scala> val relationships: RDD[Edge[String]] =
     | sc.parallelize(Array(Edge(3L, 7L, "collaborator"), Edge(5L, 3L, "Advisor"), Edge(2L, 5L, "Colleague"), Edge(5L, 7L, "Pi")))
relationships: org.apache.spark.rdd.RDD[org.apache.spark.graphx.Edge[String]] = ParallelCollectionRDD[1] at parallelize at <console>:29

scala> val defaultUser = ("Joe Doe", "Missing")
defaultUser: (String, String) = (Joe Doe,Missing)

scala> val defaultUser = ("Joe Doe", "Missing")
defaultUser: (String, String) = (Joe Doe,Missing)

scala> val graph = Graph(users, relationships, defaultUser)
graph: org.apache.spark.graphx.Graph[(String, String),String] = org.apache.spark.graphx.impl.GraphImpl@ba90170

scala> graph.vertices.collect()
[Stage 0:>                                                          (0 + 0) / 2                                                                               res0: Array[(org.apache.spark.graphx.VertexId, (String, String))] = Array((2,(istoica,professor)), (3,(rxin,student)), (7,(jgonzal,postdoc)), (5,(franklin,professor)))

scala> graph.edges.collect()
res1: Array[org.apache.spark.graphx.Edge[String]] = Array(Edge(3,7,collaborator), Edge(5,3,Advisor), Edge(2,5,Colleague), Edge(5,7,Pi))

scala> graph.vertices.filter{_._2._2 = "postdoc"}.take(10)
<console>:37: error: reassignment to val
       graph.vertices.filter{_._2._2 = "postdoc"}.take(10)
                                     ^

scala> graph.vertices.filter{_._2._2 == "postdoc"}.take(10)
res3: Array[(org.apache.spark.graphx.VertexId, (String, String))] = Array((7,(jgonzal,postdoc)))

scala> graph.vertices.filter{_._2._2 == "postdoc"}.take(10).map(_._2._1)
res4: Array[String] = Array(jgonzal)

scala> graph.vertices.filter{_._2._2 == "postdoc"}.count()
res5: Long = 1

scala> graph.vertices.filter{_._2._1 == "jgonzal"}.count()
res6: Long = 1

scala> graph.edges.filter(e => e.srcId > e.dstId).count
res7: Long = 1

scala> graph.edges.filter(e => e.srcid > e.dstId).map(_._2._1)
<console>:37: error: value srcid is not a member of org.apache.spark.graphx.Edge[String]
       graph.edges.filter(e => e.srcid > e.dstId).map(_._2._1)
                                 ^

scala> graph.edges.filter(e => e.srcId > e.dstId).map(_._2._1)
<console>:37: error: value _2 is not a member of org.apache.spark.graphx.Edge[String]
       graph.edges.filter(e => e.srcId > e.dstId).map(_._2._1)
                                                        ^

scala> graph.edges.filter(e => e.srcId > e.dstId).map(e => (e.srcId, e.attr)).join(users).map(_._2._2._1_.take(10)
     | )
<console>:37: error: value _1_ is not a member of (String, String)
       graph.edges.filter(e => e.srcId > e.dstId).map(e => (e.srcId, e.attr)).join(users).map(_._2._2._1_.take(10)
                                                                                                      ^

scala> graph.edges.filter(e => e.srcId > e.dstId).map(e => (e.srcId, e.attr)).join(users).map(_._2._2._1).take(10)
res11: Array[String] = Array(franklin)

scala> graph.edges.filter(e => e.srcId > e.dstId).map(e => (e.srcId, e.attr)).join(users)
res12: org.apache.spark.rdd.RDD[(org.apache.spark.graphx.VertexId, (String, (String, String)))] = MapPartitionsRDD[36] at join at <console>:37

scala> graph.edges.filter(e => e.srcId > e.dstId).map(e => (e.srcId, e.attr)).join(users).map(_._2._2._2).take(10)
res13: Array[String] = Array(professor)

scala> graph.edges.filter(e => e.srcId > e.dstId).map(e => (e.srcId, e.attr)).join(users).map(_._2._2._1).take(10)
res14: Array[String] = Array(franklin)

scala> graph.triplets.map(triplet => triplet.srcAttr._1 + "is the " + triplet.attr + " of " + triplet.destAttr._1).foreach(println(_))
<console>:37: error: value destAttr is not a member of org.apache.spark.graphx.EdgeTriplet[(String, String),String]
       graph.triplets.map(triplet => triplet.srcAttr._1 + "is the " + triplet.attr + " of " + triplet.destAttr._1).foreach(println(_))
                                                                                                      ^

scala> graph.triplets.map(triplet => triplet.srcAttr._1 + "is the " + triplet.attr + " of " + triplet.destAttr._1).collect.foreach(println(_))
<console>:37: error: value destAttr is not a member of org.apache.spark.graphx.EdgeTriplet[(String, String),String]
       graph.triplets.map(triplet => triplet.srcAttr._1 + "is the " + triplet.attr + " of " + triplet.destAttr._1).collect.foreach(println(_))
                                                                                                      ^

scala> graph.triplets.map(triplet => triplet.srcAttr._1 + "is the " + triplet.attr + " of " + triplet.dstAttr._1).collect.foreach(println(_))
[Stage 44:>                                                         (0 + 0) / 2                                                                               rxinis the collaborator of jgonzal
franklinis the Advisor of rxin
istoicais the Colleague of franklin
franklinis the Pi of jgonzal

scala> graph.triplets.map(triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1).collect.foreach(println(_))
rxin is the collaborator of jgonzal
franklin is the Advisor of rxin
istoica is the Colleague of franklin
franklin is the Pi of jgonzal

scala> 

