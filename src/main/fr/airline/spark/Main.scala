package fr.airline.spark

import org.apache.spark.rdd.RDD
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

    println("Nombre de ligne : " + countNumberOfLine(my_cache))
    println("Nombre de vol par année : " + countNumberOfFlightPerYear(my_cache).collect().mkString(", "))
    println("Nombre de vol par mois : " + countNumberOfFlightPerMonth(my_cache).collect().mkString(", "))
    println("Nombre de vol par jour de la semaine : " + countNumberOfFlightPerDayOfWeek(my_cache).collect().mkString(", "))
    println("Moyenne de temps de vol : " + meanDurationOfFlight(my_cache))
    println("Moyenne de temps de vol par année : " + meanDurationOfFlightPerYear(my_cache).collect().mkString(", "))
    println("Moyenne de la distance de vol : " + meanDistanceOfFlight(my_cache))
    println("Nombre de vol par compagnie aérienne : " + countNumberOfFlightPerAirline(my_cache).collect().mkString(", "))
    println("Nombre de vol par compagnie aérienne par année : " + countNumberOfFlightPerAirlinePerYear(my_cache).collect().mkString(", "))
    println("Les destinations selon leurs fréquentations : " + countDestination(my_cache).collect().mkString(", "))


    spark.stop()
  }

  /**
   * Compte le nombre de ligne dans le fichier
   * @param airline
   * @return
   */
  def countNumberOfLine(airline: RDD[Airline]): Long = {
    airline.count();
  }

  /**
   * Compte le nombre de vol par année
   * @param airline
   * @return
   */
  def countNumberOfFlightPerYear(airline: RDD[Airline]): RDD[(Int, Long)] = {
    airline
      .map(airline => (airline.get_year, 1.toLong))
      .reduceByKey(_ + _)
  }

  /**
   * Compte le nombre de vol par mois
   * @param airline
   * @return
   */
  def countNumberOfFlightPerMonth(airline: RDD[Airline]): RDD[(Int, Long)] = {
    airline
      .map(airline => (airline.get_month, 1.toLong))
      .reduceByKey(_ + _)
  }

  /**
   * Compte le nombre de vol par jour de la semaine
   * @param airline
   * @return
   */
  def countNumberOfFlightPerDayOfWeek(airline: RDD[Airline]): RDD[(Int, Long)] = {
    airline
      .map(airline => (airline.get_day_of_week, 1.toLong))
      .reduceByKey(_ + _)
  }

  /**
   * Mesure la moyenne de temps de vol
   * @param airline
   * @return
   */
  def meanDurationOfFlight(airline: RDD[Airline]): Double = {
    airline
      .map(airline => airline.get_actual_elapsed_time.toLong)
      .mean()
  }

  /**
   * Mesure la moyenne de temps de vol par année
   * @param airline
   * @return
   */
  def meanDurationOfFlightPerYear(airline: RDD[Airline]): RDD[(Int, Double)] = {
    airline
      .map(airline => (airline.get_year, airline.get_actual_elapsed_time.toLong))
      .groupByKey()
      .mapValues(_.sum)
      .mapValues(_.toDouble)
  }

  /**
   * Mesure la moyenne de la distance de vol
   * @param airline
   * @return
   */
  def meanDistanceOfFlight(airline: RDD[Airline]): Double = {
    airline
      .map(airline => airline.get_distance.toLong)
      .mean()
  }


  /**
   * Compte le nombre de vol par origine
   * @param airline
   * @return
   */
  def countDestination(airline: RDD[Airline]): RDD[(String, Long)] = {
    airline
      .map(airline => (airline.get_dest, 1.toLong))
      .reduceByKey(_ + _)
  }

  def countNumberOfFlightPerAirlinePerYear(airline: RDD[Airline]): RDD[((String, Int), Long)] = {
    airline
      .map(airline => ((airline.get_unique_carrier, airline.get_year), 1.toLong))
      .reduceByKey(_ + _)
  }

  def countNumberOfFlightPerAirline(airline: RDD[Airline]): RDD[(String, Long)] = {
    airline
      .map(airline => (airline.get_unique_carrier, 1.toLong))
      .reduceByKey(_ + _)
  }
}
