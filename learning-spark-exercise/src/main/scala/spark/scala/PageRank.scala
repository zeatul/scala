package spark.scala

import org.apache.spark._
import org.apache.spark.SparkContext._

object PageRank {
  /**
   *
   */
  def main(args: Array[String]): Unit = {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _               => "local"
    }
    val sc = new SparkContext(master, "PerKeyAvg", System.getenv("SPARK_HOME"))

    // Assume that our neighbor list was saved as a Spark objectFile
    val links = sc.objectFile[(String, Seq[String])]("links").partitionBy(new HashPartitioner(100)).persist()

    // Initialize each page's rank to 1.0; since we use mapValues, the resulting RDD will have the same partitioner as links
    var ranks = links.mapValues(v => 1.0)

    // Run 10 iterations of PageRank
    for (i <- 0 until 10) {
      val contributions = links.join(ranks).flatMap {
        case (pageId, (links, rank)) => links.map(dest => (dest, rank / links.size))
      }
      ranks = contributions.reduceByKey((x, y) => x + y).mapValues { v => 0.15 + 0.85 * v }
    }

    // Write out the final ranks
    ranks.saveAsTextFile("ranks")
  }

  /*  1. Notice that the links RDD is joined against ranks on each iteration. Since links
    is a static dataset, we partition it at the start with partitionBy(), so that it does
    not need to be shuffled across the network. In practice, the links RDD is also
    likely to be much larger in terms of bytes than ranks, since it contains a list of
    neighbors for each page ID instead of just a Double, so this optimization saves 
    considerable network traffic over a simple implementation of PageRank (e.g., in
    plain MapReduce).
    2. For the same reason, we call persist() on links to keep it in RAM across
    iterations.
    3. When we first create ranks, we use mapValues() instead of map() to preserve the
    partitioning of the parent RDD (links), so that our first join against it is cheap.
    4. In the loop body, we follow our reduceByKey() with mapValues(); because the
    result of reduceByKey() is already hash-partitioned, this will make it more efficient
    to join the mapped result against links on the next iteration.*/
}