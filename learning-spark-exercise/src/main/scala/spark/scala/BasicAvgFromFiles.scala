package spark.scala

import org.apache.spark._
import org.apache.spark.SparkContext._
class BasicAvgFromFiles {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Usage: [sparkmaster] [inputdirectory] [outputdirectory]")
      exit(1)
    }
    val master = args(0)
    val inputFile = args(1)
    val outputFile = args(2)

    val sc = new SparkContext(master, "BasicAvgFromFiles", System.getenv(""))

    val input = sc.wholeTextFiles("file://home/holden/salesFiles")
    val result = input.mapValues { y =>
      val nums = y.split("").map(x => x.toDouble)
      nums.sum / nums.size.toDouble
    }
  }

}