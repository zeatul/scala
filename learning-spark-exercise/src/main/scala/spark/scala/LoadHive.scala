package spark.scala

import org.apache.spark._
import org.apache.spark.sql.hive.HiveContext

object LoadHive {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: [sparkmaster] [tablename]")
      exit(1)
    }

    val master = args(0)
    val talbe = args(1)

    val sc = new SparkContext(master, "LoadHive", System.getenv("SPARK_HOME"))

    val hiveCtx = new HiveContext(sc)

    //    val rows = hiveCtx.sql("SELECT name, age FROM users")

    val input = hiveCtx.sql("FROM src SELECT key, value")

    val data = input.map(_.getInt(0))

    println(data.collect().toList)

  }
}