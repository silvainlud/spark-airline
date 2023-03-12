package fr.airline.spark

class ActRef extends Serializable{
  private var code: String = null
  private var brand: String = null
  private var model: String = null

  def this(code: String, brand: String, model: String) {
    this()
    this.code = code
    this.brand = brand
    this.model = model
  }

  def getCode: String = code

  def setCode(code: String): Unit = {
    this.code = code
  }

  def getBrand: String = brand

  def setBrand(brand: String): Unit = {
    this.brand = brand
  }

  def getModel: String = model

  def setModel(model: String): Unit = {
    this.model = model
  }
}

object ActRef {
  def apply(code: String, brand: String, model: String): ActRef = new ActRef(code, brand, model)

  def fromColumn(columns: Array[String]) = new ActRef(
    parseText(columns, 0),
    parseText(columns, 1),
    parseText(columns, 2),
  );

  private def parseText(columns: Array[String], x: Int) = columns(x)
}
