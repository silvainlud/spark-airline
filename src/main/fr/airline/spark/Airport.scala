package fr.airline.spark

class Airport extends Serializable {
  private var ident: String = null
  private var `type`: String = null
  private var name: String = null
  private var elevation_ft: String = null
  private var continent: String = null
  private var iso_country: String = null
  private var iso_region: String = null
  private var municipality: String = null
  private var gps_code: String = null
  private var iata_code: String = null
  private var local_code: String = null
  private var coordinates: String = null

  def this(ident: String, `type`: String, name: String, elevation_ft: String, continent: String, iso_country: String, iso_region: String, municipality: String, gps_code: String, iata_code: String, local_code: String, coordinates: String) {
    this()
    this.ident = ident
    this.`type` = `type`
    this.name = name
    this.elevation_ft = elevation_ft
    this.continent = continent
    this.iso_country = iso_country
    this.iso_region = iso_region
    this.municipality = municipality
    this.gps_code = gps_code
    this.iata_code = iata_code
    this.local_code = local_code
    this.coordinates = coordinates
  }

  def getIdent: String = ident

  def setIdent(ident: String): Unit = {
    this.ident = ident
  }

  def getType: String = `type`

  def setType(`type`: String): Unit = {
    this.`type` = `type`
  }

  def getName: String = name

  def setName(name: String): Unit = {
    this.name = name
  }

  def getElevation_ft: String = elevation_ft

  def setElevation_ft(elevation_ft: String): Unit = {
    this.elevation_ft = elevation_ft
  }

  def getContinent: String = continent

  def setContinent(continent: String): Unit = {
    this.continent = continent
  }

  def getIso_country: String = iso_country

  def setIso_country(iso_country: String): Unit = {
    this.iso_country = iso_country
  }

  def getIso_region: String = iso_region

  def setIso_region(iso_region: String): Unit = {
    this.iso_region = iso_region
  }

  def getMunicipality: String = municipality

  def setMunicipality(municipality: String): Unit = {
    this.municipality = municipality
  }

  def getGps_code: String = gps_code

  def setGps_code(gps_code: String): Unit = {
    this.gps_code = gps_code
  }

  def getIata_code: String = iata_code

  def setIata_code(iata_code: String): Unit = {
    this.iata_code = iata_code
  }

  def getLocal_code: String = local_code

  def setLocal_code(local_code: String): Unit = {
    this.local_code = local_code
  }

  def getCoordinates: String = coordinates

  def setCoordinates(coordinates: String): Unit = {
    this.coordinates = coordinates
  }
}

object Airport{
  var EMPTY = new Airport()


  def fromColumn(columns: Array[String]) = new Airport(
    parseText(columns, 0),
    parseText(columns, 1),
    parseText(columns, 2),
    parseText(columns, 3),
    parseText(columns, 4),
    parseText(columns, 5),
    parseText(columns, 6),
    parseText(columns, 7),
    parseText(columns, 8),
    parseText(columns, 9),
    parseText(columns, 10),
    parseText(columns, 11)
  );
  private def parseText(columns: Array[String], x: Int) = columns(x)

  private def parseInt(columns: Array[String], x: Int) = if (columns(x) == "NA") 0 else columns(x).toInt

  private def parseBoolean(columns: Array[String], x: Int) = if (columns(x) == "NA") false else columns(x).toInt
}
