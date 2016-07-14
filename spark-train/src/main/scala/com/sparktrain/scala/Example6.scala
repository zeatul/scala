//package com.sparktrain.scala
//
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//import org.apache.spark.SparkContext._
//
//import com.fasterxml.jackson.module.scala.DefaultScalaModule
//import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
//import com.fasterxml.jackson.databind.ObjectMapper
//import com.fasterxml.jackson.databind.DeserializationFeature
//
//import org.eclipse.jetty.client.ContentExchange
//import org.eclipse.jetty.client.HttpClient
//
///**
// * Charpter 6. Advanced Spark Programming
// */
//object Example6 {
//
//  val conf = new SparkConf().setMaster("local").setAppName("My app")
//  val sc = new SparkContext(conf)
//
//  def main(args: Array[String]): Unit = {
//    val master = args(0)
//    val inputFile = args(1)
//    val inputFile2 = args(2)
//    val outputDir = args(3)
//
//    val sc = new SparkContext(master, "AdvancedSparkProgramming", System.getenv("SPARK_HOME"))
//
//    val file = sc.textFile(inputFile)
//    val count = sc.accumulator(0)
//
//    //Example 6-3. Accumulator empty line count in Scala
//    val blankLines = sc.accumulator(0)
//    val callSigns_1 = file.flatMap {
//      line =>
//        {
//          if (line == "") blankLines += 1
//        }
//        line.split(" ")
//    }
//    callSigns_1.saveAsTextFile("output.txt")
//    println("Blank Lines: " + blankLines.value)
//
//    // side-effecting only
//    file.foreach { line =>
//      {
//        if (line.contains("KK6JKQ")) count += 1
//      }
//    }
//
//    println("Lines with 'KK6JKQ': " + count.value)
//    // Create Accumulator[Int] initialized to 0
//    val errorLines = sc.accumulator(0)
//    val dataLines = sc.accumulator(0)
//    val validSignCount = sc.accumulator(0)
//    val invalidSignCount = sc.accumulator(0)
//    val unknownCountry = sc.accumulator(0)
//    val resolvedCountry = sc.accumulator(0)
//
//    //compute errorlines and datalines
//    val callSigns = file.flatMap { line =>
//      {
//        if (line == "")
//          errorLines += 1
//        else
//          dataLines += 1
//      }
//      line.split(" ")
//    }
//
//    // Validate a call sign
//    val callSignRegex = "\\A\\d?[a-zA-Z]{1,2}\\d{1,4}[a-zA-Z]{1,3}\\Z".r
//    val validSigns = callSigns.filter {
//      sign =>
//        {
//          if ((callSignRegex findFirstIn sign).nonEmpty) {
//            validSignCount += 1
//            true
//          } else {
//            invalidSignCount += 1
//            false
//          }
//        }
//    }
//
//    val contactCounts = validSigns.map(callSign => (callSign, 1)).reduceByKey((x, y) => x + y)
//    // Force evaluation so the counters are populated
//    contactCounts.count()
//    if (invalidSignCount.value < 0.5 * validSignCount.value) {
//      contactCounts.saveAsTextFile(outputDir + "output.txt")
//    } else {
//      println(s"Too many errors ${invalidSignCount.value} for ${validSignCount.value}")
//      exit(1)
//    }
//
//    //Example 6-8. Country lookup with Broadcast values in Scala
//    // Lookup the countries for each call sign for the contactCounts RDD.  We load an array of call sign prefixes to country code to support this lookup.
//    val signPrefixes = sc.broadcast(loadCallSignTable)
//    val countryContactCounts = contactCounts.map {
//      case (sign, count) => {
//        val country = lookupInArray(sign, signPrefixes.value)
//        (country, count)
//      }
//    }.reduceByKey { (x, y) => x + y }
//    countryContactCounts.saveAsTextFile(outputDir + "output.txt")
//
//    // Resolve call signs in a second file to location
//    val countryContract2 = sc.textFile(inputFile2)
//      .flatMap(_.split("\\s+"))
//      .map {
//        case sign => {
//          val country = lookupInArray(sign, signPrefixes.value)
//          (country, 1)
//        }
//      }.reduceByKey((x, y) => x + y).collect()
//
//    // Look up the location info using a connection pool
//    val contactsContactLists = validSigns.distinct().mapPartitions {
//      signs =>
//        {
//          val mapper = createMapper()
//          // create a connection pool
//          val client = new HttpClient()
//          client.start()
//          // create http request
//          signs.map {
//            sign =>
//              {
//                createExchangeForSign(client, sign)
//              } // fetch responses
//          }.map {
//            case (sign, exchange) => (sign, readExchangeCallLog(mapper, exchange))
//          }.filter {
//            x => x._2 != null // Remove empty CallLogs
//          }
//        }
//    }
//
//     println(contactsContactLists.collect().toList)
//  }
//
//  def createExchangeForSign(client: HttpClient, sign: String): (String, ContentExchange) = {
//    val exchange = new ContentExchange()
//    exchange.setURL(s"http://new73s.herokuapp.com/qsos/${sign}.json")
//    client.send(exchange)
//    (sign, exchange)
//  }
//
//  def readExchangeCallLog(mapper: ObjectMapper, exchange: ContentExchange): Array[CallLog] = {
//    exchange.waitForDone()
//    val responseJson = exchange.getResponseContent()
//    val qsos = mapper.readValue(responseJson, classOf[Array[CallLog]])
//    qsos
//  }
//
//  def loadCallSignTable() = {
//    scala.io.Source.fromFile("./files/callsign_tbl_sorted").getLines()
//      .filter(_ != "").toArray
//  }
//
//  def lookupInArray(sign: String, prefixArray: Array[String]): String = {
//    val pos = java.util.Arrays.binarySearch(prefixArray.asInstanceOf[Array[AnyRef]], sign) match {
//      case x if x < 0 => -x - 1
//      case x          => x
//    }
//    // The country is the second element separated by comma
//    prefixArray(pos).split(",")(1)
//  }
//
//  def createMapper() = {
//    val mapper = new ObjectMapper()
//    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
//    mapper.registerModule(DefaultScalaModule)
//    mapper
//  }
//
//}