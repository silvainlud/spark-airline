package fr.airline.spark.model

class RegisteredPlane extends Serializable {
  private var serialNumber: String = null
  private var name: String = null
  private var MfrMDlCode: String = null

  def this(serialNumber: String, name: String, MfrMDlCode: String) {
    this()
    this.serialNumber = serialNumber
    this.name = name
    this.MfrMDlCode = MfrMDlCode
  }

  def getSerialNumber: String = serialNumber

  def setSerialNumber(serialNumber: String): Unit = {
    this.serialNumber = serialNumber
  }

  def getName: String = name

  def setName(name: String): Unit = {
    this.name = name
  }

  def getMfrMDlCode: String = MfrMDlCode

  def setMfrMDlCode(mfrMDlCode: String): Unit = {
    MfrMDlCode = mfrMDlCode
  }
}

object RegisteredPlane {
  def apply(serialNumber: String, name: String, MfrMDlCode: String): RegisteredPlane = new RegisteredPlane(serialNumber, name, MfrMDlCode)

  def fromColumn(columns: Array[String]) = new RegisteredPlane(
    parseText(columns, 0),
    parseText(columns, 4),
    parseText(columns, 2),
  );

  private def parseText(columns: Array[String], x: Int) = columns(x)
}