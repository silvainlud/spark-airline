package fr.airline.spark

import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val file = spark.read.textFile(args(0)).cache()

    implicit val airlineEncoder: Encoder[Airline] = Encoders.bean[Airline](classOf[Airline])

    var result = file
      .map(s => Airline.fromColumn(s.split(",")))
      .count()

    println("Count : " + result)

    spark.stop();
  }
}
