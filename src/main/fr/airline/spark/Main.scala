package fr.airline.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession}

object Main {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._

    val airlineFile = spark.read.textFile(args(0)).cache()
    val airportFile = spark.read.textFile(args(1)).cache()
    val registeredPlaneFile = spark.read.textFile(args(2)).cache()
    val actRefFile = spark.read.textFile(args(3)).cache()


    implicit val airlineEncoder: Encoder[Airline] = Encoders.bean[Airline](classOf[Airline])
    implicit val airportEncoder: Encoder[Airport] = Encoders.bean[Airport](classOf[Airport])
    implicit val registeredPlaneEncoder: Encoder[RegisteredPlane] = Encoders.bean[RegisteredPlane](classOf[RegisteredPlane])
    implicit val actRefEncoder: Encoder[ActRef] = Encoders.bean[ActRef](classOf[ActRef])

    // --- Chargement des données ---


    val registeredPlane_cache = registeredPlaneFile
      .map(s => RegisteredPlane.fromColumn(s.split(",")))
      .rdd.cache()

    val actRef_cache = actRefFile
      .map(s => ActRef.fromColumn(s.split(",")))
      .rdd.cache()

    val actRef_pre_joined = actRef_cache.map(x => (x.getCode, x));


    val registeredPlaneJoin = registeredPlane_cache
      .map(x => (x.getMfrMDlCode, x))
      .join(actRef_pre_joined)
      .map(x => (x._2._1.getSerialNumber, x._2._2.getBrand, x._2._2.getModel))
      .toDF("serial_number", "brand", "model")


    val airport_cache = airportFile
      .map(s => Airport.fromColumn(s.split(",")))
      .rdd
    var airline_cache = airlineFile
      .map(s => Airline.fromColumn(s.split(",")))
      .rdd


    // --- Static ---


    println("Nombre de ligne : " + countNumberOfLine(airline_cache))
    println("Nombre de vol par année : " + countNumberOfFlightPerYear(airline_cache).collect().mkString(", "))
    println("Nombre de vol par mois : " + countNumberOfFlightPerMonth(airline_cache).collect().mkString(", "))
    println("Nombre de vol par jour de la semaine : " + countNumberOfFlightPerDayOfWeek(airline_cache).collect().mkString(", "))
    println("Moyenne de temps de vol : " + meanDurationOfFlight(airline_cache))
    println("Moyenne de temps de vol par année : " + meanDurationOfFlightPerYear(airline_cache).collect().mkString(", "))
    println("Moyenne de la distance de vol : " + meanDistanceOfFlight(airline_cache))
    println("Nombre de vol par compagnie aérienne : " + countNumberOfFlightPerAirline(airline_cache).collect().mkString(", "))
    println("Nombre de vol par compagnie aérienne par année : " + countNumberOfFlightPerAirlinePerYear(airline_cache).collect().mkString(", "))
    println("Les destinations selon leurs fréquentations : " + countDestination(airline_cache).collect().mkString(", "))


    // --- Jointure ---
    airline_cache = airline_cache.map(x => {
      x.set_tail_num(Airline.get_serial_number(x.get_tail_num))
      x
    })

    val airlineDF = airline_cache.toDF("actual_elapsed_time", "air_time", "arr_delay", "arr_time", "cancellation_code", "cancelled", "carrier_delay", "crs_arr_time", "crs_dep_time", "crs_elapsed_time", "day_of_month", "day_of_week", "dep_delay", "dep_time", "dest", "distance", "diverted", "flight_num", "late_aircraft_delay", "month", "nas_delay", "origin", "security_delay", "tail_num", "taxi_in", "taxi_out", "unique_carrier", "weather_delay", "year")


    def airportDfWithPrefix(df: RDD[Airport], prefix: String): DataFrame = {
      df.toDF(prefix + "_continent", prefix + "_coordinates", prefix + "_elevation_ft", prefix + "_gps_code", prefix + "_iata_code", prefix + "_ident", prefix + "_iso_country", prefix + "_iso_region", prefix + "_local_code", prefix + "_municipality", prefix + "_name", prefix + "_type")
    }

    val airportOriginDf = airportDfWithPrefix(airport_cache, "origin")
    val airportDestinationDf = airportDfWithPrefix(airport_cache, "dest")
    val finalVar = airlineDF
      .join(airportOriginDf, airlineDF("origin") === airportOriginDf("origin_iata_code"), "left_outer")
      .join(airportDestinationDf, airlineDF("dest") === airportDestinationDf("dest_iata_code"), "left_outer")
      .join(registeredPlaneJoin, airlineDF("tail_num") === registeredPlaneJoin("serial_number"), "inner")


    // --- Exportation des données ---
    toParquet(finalVar, args(4))


    spark.stop()
  }

  /**
   * Exporte le dataframe en parquet
   *
   * @param df
   * @param dest
   */
  def toParquet(df: DataFrame, dest: String): Unit = {
    df.write.parquet(dest)
  }

  /**
   * Compte le nombre de ligne dans le fichier
   *
   * @param airline
   * @return
   */
  def countNumberOfLine(airline: RDD[Airline]): Long = {
    airline.count();
  }

  /**
   * Compte le nombre de vol par année
   *
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
   *
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
   *
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
   *
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
   *
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
   *
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
   *
   * @param airline
   * @return
   */
  def countDestination(airline: RDD[Airline]): RDD[(String, Long)] = {
    airline
      .map(airline => (airline.get_dest, 1.toLong))
      .reduceByKey(_ + _)
  }

  /**
   * Compte le nombre de vol par compagnie aérienne par année
   *
   * @param airline
   * @return
   */
  def countNumberOfFlightPerAirlinePerYear(airline: RDD[Airline]): RDD[((String, Int), Long)] = {
    airline
      .map(airline => ((airline.get_unique_carrier, airline.get_year), 1.toLong))
      .reduceByKey(_ + _)
  }

  /**
   * Compte le nombre de vol par compagnie aérienne
   *
   * @param airline
   * @return
   */
  def countNumberOfFlightPerAirline(airline: RDD[Airline]): RDD[(String, Long)] = {
    airline
      .map(airline => (airline.get_unique_carrier, 1.toLong))
      .reduceByKey(_ + _)
  }
}
