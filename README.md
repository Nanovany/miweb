package mx.com.santander.main

import org.apache.spark.sql.SparkSession

object HolaMundo {

  def main(args: Array[String]): Unit = {

    println("Scala-Spark")
    val sparkSession = SparkSession.builder()
      .appName("HolaMundo")
      //.master("local[*]")
      .getOrCreate()
     //spark.sparkContext.setLogLevel("WARN")

    //Create RDD.
    val data=sparkSession.sparkContext.parallelize(List(" James  reid  ", " TonyA N.  jett", "william  Ritter ", " Tiffany  harris"))

    println(data.collect().mkString("\n"))
    println("------------------------")
    //Transforming data.
      val cleanData=data.map{name =>
        val removeSpaces = name.trim.replaceAll("\\s+"," ")
        val formatNames = removeSpaces.split(" ").map(_.capitalize).mkString(" ")
        formatNames
      }
      println(cleanData.collect().mkString("\n"))
      sparkSession.stop()
  }
}
