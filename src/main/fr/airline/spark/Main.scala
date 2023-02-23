package fr.airline.spark

import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val file = spark.read.textFile(args(0)).cache()

    implicit val airlineEncoder: Encoder[Airline] = Encoders.bean[Airline](classOf[Airline])


    val my_cache = file
      .map(s => Airline.fromColumn(s.split(",")))
      .rdd
      .cache()

    val result = my_cache
      .filter(airline => airline.get_cancelled == 0 && airline.get_distance != 0)
      .count()

    println("Count : " + result)

    val dist_sum = my_cache
      .cache()
      .aggregate(0.toLong)(
        (acc, airline) => acc + airline.get_distance.toLong,
        (acc1, acc2) => acc1 + acc2
      )

    println("Sum : " + dist_sum)
    println("Mean : " + dist_sum / result)

    spark.stop()
  }
}
