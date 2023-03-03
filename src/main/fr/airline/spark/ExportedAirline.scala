package fr.airline.spark

class ExportedAirline extends Airline {

  private var origin_airport: String = null
  private var origin_region: String = null
  private var origin_municipality: String = null
  private var origin_gps_code: String = null
  private var origin_coordinates_lat: Double = 0.0
  private var origin_coordinates_long: Double = 0.0

  private var dest_airport: String = null
  private var dest_region: String = null
  private var dest_municipality: String = null
  private var dest_gps_code: String = null
  private var dest_coordinates_lat: Double = 0.0
  private var dest_coordinates_long: Double = 0.0

  def this(airline: Airline, originAirport: Airport) = {
    this()

    //Set Origin Airport Values
    this.origin_airport = originAirport.getName
    this.origin_region = originAirport.getIso_region
    this.origin_municipality = originAirport.getMunicipality
    this.origin_gps_code = originAirport.getGps_code
    var originCoordinates = originAirport.getCoordinates.split(" ").map(_.replace("\"", "").toDouble)
    if (originCoordinates.length == 2) {
      this.origin_coordinates_lat = originCoordinates(0)
      this.origin_coordinates_long = originCoordinates(1)
    }


    this.set_year(airline.get_year)
    this.set_month(airline.get_month)
    this.set_day_of_month(airline.get_day_of_month)
    this.set_day_of_week(airline.get_day_of_week)
    this.set_dep_time(airline.get_dep_time)
    this.set_crs_dep_time(airline.get_crs_dep_time)
    this.set_arr_time(airline.get_arr_time)
    this.set_crs_arr_time(airline.get_crs_arr_time)
    this.set_unique_carrier(airline.get_unique_carrier)
    this.set_flight_num(airline.get_flight_num)
    this.set_tail_num(airline.get_tail_num)
    this.set_actual_elapsed_time(airline.get_actual_elapsed_time)
    this.set_crs_elapsed_time(airline.get_crs_elapsed_time)
    this.set_air_time(airline.get_air_time)
    this.set_arr_delay(airline.get_arr_delay)
    this.set_dep_delay(airline.get_dep_delay)
    this.set_origin(airline.get_origin)
    this.set_dest(airline.get_dest)
    this.set_distance(airline.get_distance)
    this.set_taxi_in(airline.get_taxi_in)
    this.set_taxi_out(airline.get_taxi_out)
    this.set_cancelled(airline.get_cancelled)
    this.set_cancellation_code(airline.get_cancellation_code)
    this.set_diverted(airline.get_diverted)
    this.set_carrier_delay(airline.get_carrier_delay)
    this.set_weather_delay(airline.get_weather_delay)
    this.set_nas_delay(airline.get_nas_delay)
    this.set_security_delay(airline.get_security_delay)
    this.set_late_aircraft_delay(airline.get_late_aircraft_delay)


  }


}


object ExportedAirline {
  var EMPTY = new ExportedAirline()

  def apply(airline: Airline, originAirport: Airport): ExportedAirline = new ExportedAirline(airline, originAirport)

}
