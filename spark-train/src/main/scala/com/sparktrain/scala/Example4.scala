package com.sparktrain.scala


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.HashPartitioner
import org.apache.spark.Partitioner

object Example4 {
  val conf = new SparkConf().setMaster("local").setAppName("My app")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {

  }

  /**
   * Example 4-2. Creating a pair RDD using the first word as the key in Scala
   */
  def example4_2(): Unit = {
    val lines = sc.parallelize(List("hello world", "who am I", "Send me a mail"));
    val pairs = lines.map { x => (x.split(" ")(0), x) }

  }

  /**
   * Example 4-5. Simple filter on second element in Scala
   */
  def example4_5(): Unit = {
    val lines = sc.parallelize(List("hello world", "who am I", "Send me a mail"));
    val pairs = lines.map { x => (x.split(" ")(0), x) }
    val result = pairs.filter { case (key, value) => value.length() < 15 }
  }

  /**
   * Example 4-8. Per-key average with reduceByKey() and mapValues() in Scala
   */
  def example4_8(): Unit = {
    val rdd = sc.parallelize(List(("panda", 0), ("pink", 3), ("pirate", 3), ("panda", 1), ("pink", 4)))
    rdd.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
  }

  /**
   * Example 4-10. Word count in Scala
   */
  def example4_10(): Unit = {
    val input = sc.textFile("s3://....")
    val words = input.flatMap { x => x.split(" ") }
    val result = words.map(x => (x, 1)).reduceByKey((x, y) => x + y)
  }

  /**
   * Example 4-13. Per-key average using combineByKey() in Scala
   */
  def example4_13(): Unit = {
    val input = sc.parallelize(List(("panda", 0), ("pink", 3), ("pirate", 3), ("panda", 1), ("pink", 4)))
    val result = input.combineByKey(
      (v) => (v, 1),
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)).map { case (key, value) => (key, value._1 / value._2.toFloat) }
    result.collectAsMap().map(print)
  }

  /**
   * Example 4-16. reduceByKey() with custom parallelism in Scala
   */
  def exmaple4_16(): Unit = {
    val data = Seq(("a", 3), ("b", 4), ("a", 1))
    /**
     * Default parallelism
     */
    sc.parallelize(data).reduceByKey((x, y) => x + y)

    /**
     * Custom parallelism
     */
    sc.parallelize(data).reduceByKey((x, y) => x + y, 10)
  }

  /**
   * Example 4-17. Scala shell inner join
   */
  def example4_17(): Unit = {
    /*
    storeAddress = {
      (Store("Ritual"), "1026 Valencia St"), (Store("Philz"), "748 Van Ness Ave"),
      (Store("Philz"), "3101 24th St"), (Store("Starbucks"), "Seattle")}
      storeRating = {
      (Store("Ritual"), 4.9), (Store("Philz"), 4.8))}
      storeAddress.join(storeRating) == {
      (Store("Ritual"), ("1026 Valencia St", 4.9)),
      (Store("Philz"), ("748 Van Ness Ave", 4.8)),
      (Store("Philz"), ("3101 24th St", 4.8))}
      storeAddress.leftOuterJoin(storeRating) ==
      {(Store("Ritual"),("1026 Valencia St",Some(4.9))),
      (Store("Starbucks"),("Seattle",None)),
      (Store("Philz"),("748 Van Ness Ave",Some(4.8))),
      (Store("Philz"),("3101 24th St",Some(4.8)))}
      storeAddress.rightOuterJoin(storeRating) ==
      {(Store("Ritual"),(Some("1026 Valencia St"),4.9)),
      (Store("Philz"),(Some("748 Van Ness Ave"),4.8)),
      (Store("Philz"), (Some("3101 24th St"),4.8))}
      */
  }

  /**
   * Example 4-20. Custom sort order in Scala, sorting integers as if strings
   */
  def example4_20(): Unit = {
    val input = sc.parallelize(List((100, "100"), (99, "99")))
    implicit val sortIntegerByString = new Ordering[Int] {
      override def compare(a: Int, b: Int) = a.toString().compare(b.toString())
    }
    input.sortByKey()
  }

  /**
   * Example 4-22. Scala simple application
   */
  def example4_22(): Unit = {
    /**
     * Initialization code; we load the user info from a Hadoop SequenceFile on HDFS.
     * This distributes elements of userData by the HDFS block where they are found,
     * and doesn't provide Spark with any way of knowing in which partition a particular UserID is located.
     *
     * It's inefficient
     * This is because the join() operation,called each time processNewLogs() is invoked, does not know anything about how the keys are partitioned in the datasets. By default, this operation will hash all
     * the keys of both datasets, sending elements with the same key hash across the network to the same machine, and then join together the elements with the same key on that machine
     */
    class UserID
    class UserInfo(val topics: String)
    class LinkInfo(val topic: String)
    val userData = sc.sequenceFile("hsfs://......", classOf[UserID], classOf[UserInfo]).persist()
    def processNameLogs(logFileName: String): Unit = {
      val events = sc.sequenceFile(logFileName, classOf[UserID], classOf[LinkInfo])
      val joined = userData.join(events) // RDD of (UserID, (UserInfo, LinkInfo)) pairs
      val offTopicVisits = joined.filter { case (userId: UserID, (userInfo: UserInfo, linkInfo: LinkInfo)) => userInfo.topics.startsWith(linkInfo.topic) }.count()
      println("Number of visits to non-subscribed topics: " + offTopicVisits)
    }

    processNameLogs("myLogFileName")
  }

  /**
   * Example 4-23. Scala custom partitioner
   * compare with exmaple4_22, Fixing this is simple: just use the partitionBy() transformation on userData to hash-partition it at the start of the program
   */
  def example4_23(): Unit = {
    class UserID
    class UserInfo(val topics: String)
    class LinkInfo(val topic: String)
    // Create 100 partitions
    val userData = sc.sequenceFile[UserID, UserInfo]("hdfs://...", classOf[UserID], classOf[UserInfo]).partitionBy(new HashPartitioner(100)).persist()
    def processNameLogs(logFileName: String): Unit = {
      val events = sc.sequenceFile(logFileName, classOf[UserID], classOf[LinkInfo])
      val joined = userData.join(events) // RDD of (UserID, (UserInfo, LinkInfo)) pairs
      val offTopicVisits = joined.filter { case (userId: UserID, (userInfo: UserInfo, linkInfo: LinkInfo)) => userInfo.topics.startsWith(linkInfo.topic) }.count()
      println("Number of visits to non-subscribed topics: " + offTopicVisits)
    }

    processNameLogs("myLogFileName")
  }

  /**
   * Example 4-25. Scala PageRank : The PageRank algorithm, named after Google’s Larry Page
   *
   * 1. Initialize each page’s rank to 1.0.
   * 2. On each iteration, have page p send a contribution of rank(p)/numNeighbors(p)
   * 		to its neighbors (the pages it has links to).
   * 3. Set each page’s rank to 0.15 + 0.85 * contributionsReceived.
   */
  def example4_25(): Unit = {
    // Assume that our neighbor list was saved as a Spark objectFile
    val links = sc.objectFile[(String, Seq[String])]("links").partitionBy(new HashPartitioner(100)).persist()
    // Initialize each page's rank to 1.0; since we use mapValues, the resulting RDD
    // will have the same partitioner as links
    var ranks = links.mapValues { v => 1.0 }
    //Run 10 iterations of PageRand
    for (i <- 0 until 10){
      val contributions = links.join(ranks).flatMap{
        case(pageId,(links,rank))=>links.map(dest=>(dest,rank/links.size))
      }
      ranks = contributions.reduceByKey((x,y)=>x+y).mapValues { v => 0.15+0.85*v }
    }
    
    ranks.saveAsTextFile("ranks")
  }
  
  /**
   * Example 4-26. Scala custom partitioner 
   * shows how we would write the domain-name-based partitioner sketched previously, which hashes only the domain name of each UR
   */
  def example4_26():Unit={
    class DomainNamePartitioner(numParts:Int) extends Partitioner{
      override def numPartitions:Int = numParts
      override def getPartition(key:Any):Int={
        val domain = new java.net.URL(key.toString()).getHost
        val code = (domain.hashCode() % numPartitions)
        if (code < 0) {
          code + numPartitions
        }else{
          code
        }
      }
      
      override def equals(other:Any):Boolean = {
        other match{
          case dnp:DomainNamePartitioner => dnp.numPartitions == numPartitions
          case _=>false
        }
      }
    }
  }
}