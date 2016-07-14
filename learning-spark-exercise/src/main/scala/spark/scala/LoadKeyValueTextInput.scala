package spark.scala

import org.apache.spark._
import org.apache.hadoop.io.{MapWritable, Text}
import org.apache.hadoop.mapred.KeyValueTextInputFormat

/**
 * Example 5-24. Loading KeyValueTextInputFormat() with old-style API in Scala
 */
object LoadKeyValueTextInput {
  def main(args:Array[String]):Unit = {
     if (args.length < 2) {
      println("Usage: [sparkmaster] [inputfile]")
      exit(1)
    }
    val master = args(0)
    val inputFile = args(1)
    val sc = new SparkContext(master, "LoadKeyValueTextInput", System.getenv("SPARK_HOME"))
    val input = sc.hadoopFile[Text,Text,KeyValueTextInputFormat](inputFile).map{
      case(x,y)=>(x.toString(),y.toString())
    }
  }
  
}