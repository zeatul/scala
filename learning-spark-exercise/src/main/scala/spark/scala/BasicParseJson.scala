package spark.scala

import org.apache.spark._

object BasicParseJson {

  def main(args: Array[String]): Unit = {

    import com.fasterxml.jackson.module.scala.DefaultScalaModule
    import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
    import com.fasterxml.jackson.databind.ObjectMapper
    import com.fasterxml.jackson.databind.DeserializationFeature

    case class Person(name: String, lovesPandas: Boolean) {
      def lovePandas = true
    }

    if (args.length < 3) {
      println("Usage: [sparkmaster] [inputfile] [outputfile]")
      exit(1)
    }
    val master = args(0)
    val inputFile = args(1)
    val outputFile = args(2)

    val sc = new SparkContext(master, "BasicParseJson", System.getenv("SPARK_HOME"))
    val input = sc.textFile(inputFile)

    val mapper = new ObjectMapper()

    val result = input.flatMap(record => {
      try {
        Some(mapper.readValue(record, classOf[Person]))
      } catch {
        case e: Exception => None
      }
    })
    
    result.filter { p => p.lovesPandas }.map { mapper.writeValueAsString(_) }.saveAsTextFile(outputFile)
  }

  
}