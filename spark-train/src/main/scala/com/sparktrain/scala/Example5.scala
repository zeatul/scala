package com.sparktrain.scala


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.HashPartitioner
import org.apache.spark.Partitioner
import au.com.bytecode.opencsv.CSVReader

/**
 * Charpeter 5 loading and saving your data
 */
object Example5 {
  val conf = new SparkConf().setMaster("local").setAppName("My app")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {

  }

  /**
   * Example 5-2. Loading a text file in Scala
   */
  def example5_1(): Unit = {
    val input = sc.textFile("file:///home/holden/repos/spark/README.md")
  }

  /**
   * Example 5-4. Average value per file in Scala
   */
  def example5_4(): Unit = {
    val input = sc.wholeTextFiles("file://home/holden/salesFiles")
    val result = input.mapValues {
      y =>
        {
          val nums = y.split(" ").map { x => x.toDouble }
          nums.sum / nums.size.toDouble
        }
    }
  }

  /**
   * Example 5-7. Loading JSON in Scala
   * Example 5-10. Saving JSON in Scala
   * Example 5-19. Writing CSV in Scala
   */
  def exmaple5_7(): Unit = {
    import com.fasterxml.jackson.module.scala.DefaultScalaModule
    import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
    import com.fasterxml.jackson.databind.ObjectMapper
    import com.fasterxml.jackson.databind.DeserializationFeature

    case class Person(name: String, lovesPandas: Boolean) {
      def lovePandas = true
    }
    val input = sc.textFile("file:///.....")
    val mapper = new ObjectMapper()
    //load
    val result = input.flatMap {
      record =>
        try {
          Some(mapper.readValue(record, classOf[Person]))
        } catch {
          case e: Exception => None
        }
    }
    //write as textFile
    result.filter { p => p.lovePandas }
      .map(mapper.writeValueAsString(_))
      .saveAsTextFile("outputFile")

    //write as csv file
    //    import java.io.StringWriter
    //    import au.com.bytecode.opencsv.CSVWriter
    //    result.map { person => List(person.name,person.lovesPandas).toArray }
    //       .mapPartitions{
    //         people=>{
    //           val stringWriter = new StringWriter()
    //           val csvWriter = new CSVWriter(stringWriter)
    //           csvWriter.writeAll(people.toList)
    //           Iterator(stringWriter.toString)
    //         }
    //       }
    //       .saveAsTextFile("outFile")
  }

  /**
   * Example 5-13. Loading CSV with textFile() in Scala
   *
   */
  def exmaple5_13(): Unit = {
    import java.io.StringReader
    import au.com.bytecode.opencsv.CSVReader

    val input = sc.textFile("inputFile")
    val result = input.map {
      line =>
        {
          val reader = new CSVReader(new StringReader(line));
          reader.readNext();
        }
    }
  }

  /**
   * Example 5-16. Loading CSV in full in Scala
   * If there are embedded newlines in fields, we will need to load each file in full and parse the entire segment
   */
  def exmaple5_16(): Unit = {
    //    import java.io.StringReader
    //    import au.com.bytecode.opencsv.CSVReader
    //    case class Person(name: String, favoriteAnimal: String)
    //    val input = sc.wholeTextFiles("inputFile")
    //    val result = input.flatMap {
    //      case (_, txt) => {
    //        val reader = new CSVReader(new StringReader(txt))
    //        reader.readAll().map(x => Person(x(0), x(1)))
    //      }
    //    }
  }

  /**
   * Example 5-21. Loading a SequenceFile in Scala
   */
  def example5_21(): Unit = {
    import org.apache.hadoop.io.Text
    import org.apache.hadoop.io.IntWritable
    val data = sc.sequenceFile("inFile", classOf[Text], classOf[IntWritable]).
      map { case (x, y) => (x.toString, y.get()) }
    /*In Scala there is a convenience function that can automatically convert Writables to their corresponding Scala type. Instead of specifying the keyClass and valueClass, we can call sequenceFile[Key, Value](path, minPartitions) and get back an RDD of native Scala types.*/
		sc.sequenceFile[String, Int]("inputFile")
  }
  
  /**
   * Example 5-23. Saving a SequenceFile in Scala
   */
  def example5_23():Unit = {
    val data = sc.parallelize(List(("Panda", 3), ("Kay", 6), ("Snail", 2)))
    data.saveAsSequenceFile("outputFile")
  }
  
  /**
   * Example 5-24. Loading KeyValueTextInputFormat() with old-style API in Scala
   */
  def example5_24():Unit = {
    import org.apache.hadoop.io.Text
    import org.apache.hadoop.mapred.KeyValueTextInputFormat 
    val input = sc.hadoopFile[Text,Text,KeyValueTextInputFormat]("inputFile").map{case (x,y)=>(x.toString,y.toString)}
  }
  
  /**
   * Example 5-25. Loading LZO-compressed JSON with Elephant Bird in Scala
   * LZO support requires you to install the hadoop-lzo package and point Spark to its native libraries. If you install the Debian package, adding --driver-library-path /usr/lib/hadoop/lib/native/
   * --driver-class-path /usr/lib/hadoop/lib/ to your sparksubmit invocation should do the trick.
   */
  def example5_25():Unit = {
    import org.apache.hadoop.io.LongWritable
    import  org.apache.hadoop.io.MapWritable
//待寻找 LzoJsonInputFormat的jar包hadoop-lzo   val input = sc.newAPIHadoopFile("inputFile",classOf[LzoJsonInputFormat],classOf[LongWritable],classOf[MapWritable],conf)
  }
  
  /**
   * Example 5-28. Elephant Bird protocol buffer writeout in Scala
   */
  def example5_28():Unit = {
    
  }
}