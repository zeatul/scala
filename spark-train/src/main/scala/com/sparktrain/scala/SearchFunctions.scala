package com.sparktrain.scala

import org.apache.spark.rdd.RDD

class SearchFunctions(val query: String) {

  def isMatch(s: String): String = {
    s.contains(query).toString()
  }

  def getMatchesFunctionReference(rdd: RDD[String]): RDD[String] = {
    rdd.map { isMatch }
  }

  def getMatchesFieldReference(rdd: RDD[String]): RDD[String] = {
    rdd.flatMap(x => x.split(query))
  }

  def getMatchesNoReference(rdd: RDD[String]): RDD[String] = {
    // Safe: extract just the field we need into a local variable
    val query_ = this.query
    rdd.flatMap(x => x.split(query))
  }
}