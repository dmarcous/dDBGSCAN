package com.github.dmarcous.ddbgscan.model

import com.github.dmarcous.s2utils.converters.CoordinateConverters
import com.github.dmarcous.s2utils.s2.S2Utilities
import com.google.common.geometry.S2CellId

case class KeyGeoEntity (
  level : Int,
  s2CellId : Long,
  geoData : Array[String]
){

  def this(entity: LonLatGeoEntity, level: Int) =
    this(
      level = level,
      geoData = Array[String](entity.longitude.toString, entity.latitude.toString),
      s2CellId = entity.getS2CellId(level, Array[String](entity.longitude.toString, entity.latitude.toString))
    )
  def this(entity: S2CellIdGeoEntity, level: Int) =
    this(
      level = level,
      geoData = Array[String](entity.inputS2CellId.toString),
      s2CellId = entity.getS2CellId(level, Array[String](entity.inputS2CellId.toString))
    )
  def this(entity: S2CellTokenGeoEntity, level: Int) =
    this(
      level = level,
      geoData = Array[String](entity.inputS2CellToken),
      s2CellId = entity.getS2CellId(level, Array[String](entity.inputS2CellToken))
    )
}

sealed trait GeoToKeyEntityConverter extends Product{
  def getS2CellId(level: Int, geoData : Array[String]) : Long
}

case class LonLatGeoEntity(
  longitude : Double,
  latitude : Double
) extends GeoToKeyEntityConverter
{
  def toParams(longitude : Double, latitude : Double) : Array[String] = {
    Array[String](longitude.toString, latitude.toString)
  }
  def getS2CellId(level: Int): Long = {
    getS2CellId(level, this.toParams(this.longitude, this.latitude))
  }
  override def getS2CellId(level: Int, geoData : Array[String]): Long = {
    CoordinateConverters.lonLatToS2CellID(geoData(0).toDouble, geoData(1).toDouble, level).id()
  }
}

case class S2CellIdGeoEntity(
  inputS2CellId : Long
) extends GeoToKeyEntityConverter
{
  def toParams(inputS2CellId : Long) : Array[String] = {
    Array[String](inputS2CellId.toString)
  }
  def getS2CellId(level: Int): Long = {
    getS2CellId(level, this.toParams(this.inputS2CellId))
  }
  override def getS2CellId(level: Int, geoData : Array[String]): Long = {
    val centerPoint = S2Utilities.getCellCenter(new S2CellId(geoData(0).toLong))
    CoordinateConverters.lonLatToS2CellID(centerPoint._1, centerPoint._2, level).id()
  }
}

case class S2CellTokenGeoEntity(
  inputS2CellToken : String
) extends GeoToKeyEntityConverter
{
  def toParams(inputS2CellToken : String) : Array[String] = {
    Array[String](inputS2CellToken)
  }
  def getS2CellId(level: Int): Long = {
    getS2CellId(level, this.toParams(this.inputS2CellToken))
  }
  override def getS2CellId(level: Int, geoData : Array[String]): Long = {
    val centerPoint = S2Utilities.getCellCenter(geoData(0))
    CoordinateConverters.lonLatToS2CellID(centerPoint._1, centerPoint._2, level).id()
  }
}