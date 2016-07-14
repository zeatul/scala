package com.sparktrain.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object WordCount {
  def main(args:Array[String]):Unit = {
    
    val inputFile = args(0)
      val outputFile = args(1)
    
    println("inputFile="+inputFile);
    
    println("outputFile="+outputFile);
    val conf = new SparkConf().setAppName("wordCount")
    
    val sc = new SparkContext(conf)
    //load our input data
    val input = sc.textFile(inputFile)
    
    println("input.count="+input.count())
    
    //split it up into words
   val words = input.flatMap(line => line.split(" "))
   println("words.count="+words.count());
   
    //transform into pairs and count.
    
    val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
    
     println("counts.count="+counts.count());
    
    counts.saveAsTextFile(outputFile)
    
    
  }
}