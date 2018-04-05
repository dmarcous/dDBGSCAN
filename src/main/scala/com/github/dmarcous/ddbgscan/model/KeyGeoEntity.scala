package com.github.dmarcous.ddbgscan.model

import com.github.dmarcous.s2utils.converters.CoordinateConverters
import com.github.dmarcous.s2utils.s2.S2Utilities
import com.google.common.geometry.S2CellId

trait KeyGeoEntity {
  def getS2CellId(level: Int) : Long
}

case class LonLatGeoEntity(
  longitude : Double,
  latitude : Double
) extends KeyGeoEntity
{
  override def getS2CellId(level: Int): Long = {
    CoordinateConverters.lonLatToS2CellID(this.longitude, this.latitude, level).id()
  }
}

case class S2CellIdGeoEntity(
 s2CellId : Long
) extends KeyGeoEntity
{
  override def getS2CellId(level: Int): Long = {
    val centerPoint = S2Utilities.getCellCenter(new S2CellId(this.s2CellId))
    CoordinateConverters.lonLatToS2CellID(centerPoint._1, centerPoint._2, level).id()
  }
}

case class S2CellTokenGeoEntity(
  s2CellToken : String
) extends KeyGeoEntity
{
  override def getS2CellId(level: Int): Long = {
    val centerPoint = S2Utilities.getCellCenter(this.s2CellToken)
    CoordinateConverters.lonLatToS2CellID(centerPoint._1, centerPoint._2, level).id()
  }
}