package spark.scala

import java.io.StringReader

import org.apache.spark._
import scala.collection.JavaConversions._
import au.com.bytecode.opencsv.CSVReader

object BasicParseWholeFileCsv {
 
  case class Person(name: String, favoriteAnimal: String)
  
  def main(args: Array[String]) {
    if (args.length < 3) {
      println("Usage: [sparkmaster] [inputfile] [outputfile]")
      exit(1)
    }
    
    val master = args(0)
    val inputFile = args(1)
    val outputFile = args(2)
    val sc = new SparkContext(master, "BasicParseCsv", System.getenv("SPARK_HOME"))

    val input = sc.wholeTextFiles(inputFile)    
    val result = input.flatMap{ case(_,txt)=>
      val reader = new CSVReader(new StringReader(txt))
      reader.readAll().map { x => Person(x(0),x(1) )}
    }
    
    
  }
}