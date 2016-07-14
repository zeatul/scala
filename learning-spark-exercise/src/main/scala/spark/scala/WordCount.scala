package spark.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {
  
  def main(args: Array[String]): Unit = {
    //input parameter
    val inputFile = args(0)
    val outputFile = args(1)
    //Create a Scala Spark Context
    val conf = new SparkConf().setAppName("wordCount")
    val sc = new SparkContext(conf)
    //Load our input data
    val input = sc.textFile(inputFile)
    //Split it up into word.
    val words = input.flatMap { line => line.split("") }
    //Transform into pair and count.  
    val counts = words.map { (_,1) }.reduceByKey{_+_}
    // Save the word count back out to a text file, causing evaluation.
    counts.saveAsTextFile(outputFile)
  }
}