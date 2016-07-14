package spark.scala

import java.io.StringReader
import java.io.StringWriter

import org.apache.spark._
import scala.util.parsing.json.JSON
import scala.collection.JavaConversions._

import au.com.bytecode.opencsv.CSVReader
import au.com.bytecode.opencsv.CSVWriter
/**
 * Example 5-13. Loading CSV with textFile() in Scala
 */
object BasicParseCsv {

  case class Person(name: String, favouriteAnimal: String)

  def main(args: Array[String]) {
    if (args.length < 3) {
      println("Usage: [sparkmaster] [inputfile] [outputfile]")
      exit(1)
    }
    val master = args(0)
    val inputFile = args(1)
    val outputFile = args(2)
    val sc = new SparkContext(master, "BasicParseCsv", System.getenv("SPARK_HOME"))

    val input = sc.textFile(inputFile)

    val result = input.map { line =>
      val reader = new CSVReader(new StringReader(line));
      reader.readNext();
    }

    val people = result.map(x => Person(x(0), x(1)))

    val pandaLovers = people.filter { person => person.favouriteAnimal == "panda" }

    pandaLovers.map(person => List(person.name, person.favouriteAnimal).toArray).mapPartitions { people =>
      val stringWriter = new StringWriter();
      val csvWriter = new CSVWriter(stringWriter)
      csvWriter.writeAll(people.to)
      Iterator(stringWriter.toString)
    }.saveAsTextFile(outputFile)

  }
}