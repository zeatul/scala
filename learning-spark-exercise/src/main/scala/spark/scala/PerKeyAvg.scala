package spark.scala

import org.apache.spark.SparkContext

class PerKeyAvg {
  def main(args: Array[String]): Unit = {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _               => "local"
    }

    val sc = new SparkContext(master, "PerKeyAvg", System.getenv("SPARK_HOME"))
    val input = sc.parallelize(List(("coffee", 1), ("coffee", 2), ("panda", 4)))
    val result = input.combineByKey(
      (x) => (x, 1),
      (acc: (Int, Int), x) => (acc._1 + x, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
  }
}