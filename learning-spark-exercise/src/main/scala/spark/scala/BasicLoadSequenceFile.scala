package spark.scala

import org.apache.spark._
import org.apache.hadoop.io.{ IntWritable, Text }

/**
 * Example 5-21. Loading a SequenceFile in Scala
 */
object BasicLoadSequenceFile {
  def main(args: Array[String]): Unit = {
    val master = args(0)
    val inFile = args(1)
    val sc = new SparkContext(master, "", System.getenv("SPARK_HOME"))
    val data = sc.sequenceFile(inFile, classOf[Text], classOf[IntWritable]).map { case (x, y) => (x.toString, y.get()) }

  }
}