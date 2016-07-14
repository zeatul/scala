package com.sparktrain.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel

object Example3 {

  val conf = new SparkConf().setMaster("local").setAppName("My app")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {

    val input = sc.parallelize(List(1, 2, 3, 4))
    val result = input.map { x => x * x }
    println(result.collect().mkString(","))

  }

  /**
   * Example 3-36. aggregate() in Scala
   */
  def example3_36(): Unit = {
    val input = sc.parallelize(List(1, 2, 3, 4))
    val result2 = input.aggregate((0, 0))(
      (acc, value) => (acc._1 + value, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))

    input.aggregate((0, 0))(
      (x, y) => (x._1 + y, x._2 + 1),
      (x, y) => (x._1 + y._1, x._2 + y._2))

    val avg = result2._1 / result2._2.toDouble
  }

  /**
   * Example 3-40. persist() in Scala
   */
  def exmaple3_40(): Unit = {
    val input = sc.parallelize(List(1, 2, 3, 4))
    val result = input.map(x => x * x)
    result.persist(StorageLevel.DISK_ONLY)
    println(result.count());
    println(result.collect().mkString(","));
  }
}