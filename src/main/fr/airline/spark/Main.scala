package fr.airline.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession}
object Main {



  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    import spark.implicits._

    val airlineFile = spark.read.textFile(args(0)).cache()
    val airportFile = spark.read.textFile(args(1)).cache()





    implicit val airlineEncoder: Encoder[Airline] = Encoders.bean[Airline](classOf[Airline])
    implicit val airportEncoder: Encoder[Airport] = Encoders.bean[Airport](classOf[Airport])
    implicit val exportedAirlineEncoder: Encoder[ExportedAirline] = Encoders.bean[ExportedAirline](classOf[ExportedAirline])


    var airport_cache = airportFile
      .map(s => Airport.fromColumn(s.split(",")))
      .rdd
      .cache()


    //Count number of airport
    println("Number of airport : " + airport_cache.count())
    println("Number of airport per state : " + airport_cache.map(airport => (airport.getIso_region, 1.toLong)).reduceByKey(_ + _).collect().mkString(", "))




    val airline_cache = airlineFile
      .map(s => Airline.fromColumn(s.split(",")))
      .rdd
      .cache()

    var airportMapForJoin = airport_cache.map(airport  => (airport.getIata_code, airport))

    // Join airline with the origin airport
    val airlineWithOriginAirport = airline_cache
      .map(airline => (airline.get_origin, airline))
      .join(airportMapForJoin)
      .map(tuple => ExportedAirline.apply(tuple._2._1, tuple._2._2))
      .cache()




    toParquet(airlineWithOriginAirport.toDF(), "/airline.parquet");

//    println("Nombre de ligne : " + countNumberOfLine(airline_cache))
//    println("Nombre de vol par année : " + countNumberOfFlightPerYear(airline_cache).collect().mkString(", "))
//    println("Nombre de vol par mois : " + countNumberOfFlightPerMonth(airline_cache).collect().mkString(", "))
//    println("Nombre de vol par jour de la semaine : " + countNumberOfFlightPerDayOfWeek(airline_cache).collect().mkString(", "))
//    println("Moyenne de temps de vol : " + meanDurationOfFlight(airline_cache))
//    println("Moyenne de temps de vol par année : " + meanDurationOfFlightPerYear(airline_cache).collect().mkString(", "))
//    println("Moyenne de la distance de vol : " + meanDistanceOfFlight(airline_cache))
//    println("Nombre de vol par compagnie aérienne : " + countNumberOfFlightPerAirline(airline_cache).collect().mkString(", "))
//    println("Nombre de vol par compagnie aérienne par année : " + countNumberOfFlightPerAirlinePerYear(airline_cache).collect().mkString(", "))
//    println("Les destinations selon leurs fréquentations : " + countDestination(airline_cache).collect().mkString(", "))


    spark.stop()
  }

  def toParquet(df: DataFrame, dest: String): Unit = {
    df.write.parquet(dest)
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
