package fr.airline.spark

class Airline extends Serializable {

  private var year = 0
  private var month = 0
  private var day_of_month = 0
  private var day_of_week = 0
  private var dep_time = 0
  private var crs_dep_time = 0
  private var arr_time = 0
  private var crs_arr_time = 0
  private var unique_carrier: String = null
  private var flight_num = 0
  private var tail_num: String = null
  private var actual_elapsed_time = 0
  private var crs_elapsed_time = 0
  private var air_time = 0
  private var arr_delay = 0
  private var dep_delay = 0
  private var origin: String = null
  private var dest: String = null
  private var distance = 0
  private var taxi_in = 0
  private var taxi_out = 0
  private var cancelled = 0
  private var cancellation_code: String = null
  private var diverted = 0
  private var carrier_delay = 0
  private var weather_delay = 0
  private var nas_delay = 0
  private var security_delay = 0
  private var late_aircraft_delay = 0


  def this(actual_elapsed_time: Int, air_time: Int, arr_delay: Int, arr_time: Int, crs_arr_time: Int, crs_dep_time: Int, crs_elapsed_time: Int, cancellation_code: String,
           cancelled: Int, carrier_delay: Int, day_of_week: Int, day_of_month: Int, dep_delay: Int, dep_time: Int, dest: String, distance: Int, diverted: Int,
           flight_num: Int, late_aircraft_delay: Int, month: Int, nas_delay: Int, origin: String, security_delay: Int, tail_num: String, taxi_in: Int, taxi_out: Int,
           unique_carrier: String, weather_delay: Int, year: Int) {
    this()
    this.year = year
    this.month = month
    this.day_of_month = day_of_month
    this.day_of_week = day_of_week
    this.dep_time = dep_time
    this.crs_dep_time = crs_dep_time
    this.arr_time = arr_time
    this.crs_arr_time = crs_arr_time
    this.unique_carrier = unique_carrier
    this.flight_num = flight_num
    this.tail_num = tail_num
    this.actual_elapsed_time = actual_elapsed_time
    this.crs_elapsed_time = crs_elapsed_time
    this.air_time = air_time
    this.arr_delay = arr_delay
    this.dep_delay = dep_delay
    this.origin = origin
    this.dest = dest
    this.distance = distance
    this.taxi_in = taxi_in
    this.taxi_out = taxi_out
    this.cancelled = cancelled
    this.cancellation_code = cancellation_code
    this.diverted = diverted
    this.carrier_delay = carrier_delay
    this.weather_delay = weather_delay
    this.nas_delay = nas_delay
    this.security_delay = security_delay
    this.late_aircraft_delay = late_aircraft_delay
  }

  def get_year: Int = year;

  def set_year(year: Int): Unit = {
    this.year = year;
  }

  def get_month: Int = month;

  def set_month(month: Int): Unit = {
    this.month = month;
  }

  def get_day_of_month: Int = day_of_month;

  def set_day_of_month(day_of_month: Int): Unit = {
    this.day_of_month = day_of_month;
  }

  def get_day_of_week: Int = day_of_week;

  def set_day_of_week(day_of_week: Int): Unit = {
    this.day_of_week = day_of_week;
  }

  def get_dep_time: Int = dep_time;

  def set_dep_time(dep_time: Int): Unit = {
    this.dep_time = dep_time;
  }

  def get_crs_dep_time: Int = crs_dep_time;

  def set_crs_dep_time(crs_dep_time: Int): Unit = {
    this.crs_dep_time = crs_dep_time;
  }

  def get_arr_time: Int = arr_time;

  def set_arr_time(arr_time: Int): Unit = {
    this.arr_time = arr_time;
  }

  def get_crs_arr_time: Int = crs_arr_time;

  def set_crs_arr_time(crs_arr_time: Int): Unit = {
    this.crs_arr_time = crs_arr_time;
  }

  def get_unique_carrier: String = unique_carrier;

  def set_unique_carrier(unique_carrier: String): Unit = {
    this.unique_carrier = unique_carrier;
  }

  def get_flight_num: Int = flight_num;

  def set_flight_num(flight_num: Int): Unit = {
    this.flight_num = flight_num;
  }

  def get_tail_num: String = tail_num;

  def set_tail_num(tail_num: String): Unit = {
    this.tail_num = tail_num;
  }

  def get_actual_elapsed_time: Int = actual_elapsed_time;

  def set_actual_elapsed_time(actual_elapsed_time: Int): Unit = {
    this.actual_elapsed_time = actual_elapsed_time;
  }

  def get_crs_elapsed_time: Int = crs_elapsed_time;

  def set_crs_elapsed_time(crs_elapsed_time: Int): Unit = {
    this.crs_elapsed_time = crs_elapsed_time;
  }

  def get_air_time: Int = air_time;

  def set_air_time(air_time: Int): Unit = {
    this.air_time = air_time;
  }

  def get_arr_delay: Int = arr_delay;

  def set_arr_delay(arr_delay: Int): Unit = {
    this.arr_delay = arr_delay;
  }

  def get_dep_delay: Int = dep_delay;

  def set_dep_delay(dep_delay: Int): Unit = {
    this.dep_delay = dep_delay;
  }

  def get_origin: String = origin;

  def set_origin(origin: String): Unit = {
    this.origin = origin;
  }

  def get_dest: String = dest;

  def set_dest(dest: String): Unit = {
    this.dest = dest;
  }

  def get_distance: Int = distance;

  def set_distance(distance: Int): Unit = {
    this.distance = distance;
  }

  def get_taxi_in: Int = taxi_in;

  def set_taxi_in(taxi_in: Int): Unit = {
    this.taxi_in = taxi_in;
  }

  def get_taxi_out: Int = taxi_out;

  def set_taxi_out(taxi_out: Int): Unit = {
    this.taxi_out = taxi_out;
  }

  def get_cancelled: Int = cancelled;

  def set_cancelled(cancelled: Int): Unit = {
    this.cancelled = cancelled;
  }

  def get_cancellation_code: String = cancellation_code;

  def set_cancellation_code(cancellation_code: String): Unit = {
    this.cancellation_code = cancellation_code;
  }

  def get_diverted: Int = diverted;

  def set_diverted(diverted: Int): Unit = {
    this.diverted = diverted;
  }

  def get_carrier_delay: Int = carrier_delay;

  def set_carrier_delay(carrier_delay: Int): Unit = {
    this.carrier_delay = carrier_delay;
  }

  def get_weather_delay: Int = weather_delay;

  def set_weather_delay(weather_delay: Int): Unit = {
    this.weather_delay = weather_delay;
  }

  def get_nas_delay: Int = nas_delay;

  def set_nas_delay(nas_delay: Int): Unit = {
    this.nas_delay = nas_delay;
  }

  def get_security_delay: Int = security_delay;

  def set_security_delay(security_delay: Int): Unit = {
    this.security_delay = security_delay;
  }

  def get_late_aircraft_delay: Int = late_aircraft_delay;

  def set_late_aircraft_delay(late_aircraft_delay: Int): Unit = {
    this.late_aircraft_delay = late_aircraft_delay;
  }


}

object Airline {
  var EMPTY = new Airline

  def fromColumn(columns: Array[String]) = new Airline(
    parseInt(columns, 0),
    parseInt(columns, 1),
    parseInt(columns, 2),
    parseInt(columns, 3),
    parseInt(columns, 4),
    parseInt(columns, 5),
    parseInt(columns, 6),
    parseText(columns, 7),
    parseInt(columns, 8),
    parseInt(columns, 9),
    parseInt(columns, 10),
    parseInt(columns, 11),
    parseInt(columns, 12),
    parseInt(columns, 13),
    parseText(columns, 14),
    parseInt(columns, 15),
    parseInt(columns, 16),
    parseInt(columns, 17),
    parseInt(columns, 18),
    parseInt(columns, 19),
    parseInt(columns, 20),
    parseText(columns, 21),
    parseInt(columns, 22),
    parseText(columns, 23),
    parseInt(columns, 24),
    parseInt(columns, 25),
    parseText(columns, 26),
    parseInt(columns, 27),
    parseInt(columns, 28)
  )

  private def parseText(columns: Array[String], x: Int) = columns(x)

  private def parseInt(columns: Array[String], x: Int) = if (columns(x) == "NA") 0 else columns(x).toInt

  private def parseBoolean(columns: Array[String], x: Int) = if (columns(x) == "NA") false else columns(x).toInt

  def get_serial_number(tail_num: String): String = {
    if (tail_num == null || tail_num == "NA") {
      return null
    }
    if (tail_num.startsWith("N")) {
      return tail_num.substring(1)
    }
    return tail_num


  }
}
