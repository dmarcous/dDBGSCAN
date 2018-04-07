package com.github.dmarcous.ddbgscan.model

import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class KeyGeoEntityTest extends FlatSpec{
  val lon = 34.777112
  val lat = 32.0718015
  val s2CellIdLvl14 = 1521455257954025472L
  val s2CellTokenLvl14 = "1521455257954025472"
  val s2Lvl14 = 14
  val s2CellIdLvl15 = 1521455259027767296L
  val s2Lvl15 = 15

  "LonLatGeoEntity.getS2CellId" should "return a the right cell" in
  {
    val entity =
      LonLatGeoEntity(lon, lat)

    entity.getS2CellId(s2Lvl14) should equal(s2CellIdLvl14)
    entity.getS2CellId(s2Lvl15) should equal(s2CellIdLvl15)
  }

  "S2CellIdGeoEntity.getS2CellId" should "return a the right cell" in
  {
    val entity =
      S2CellIdGeoEntity(s2CellIdLvl15)

    entity.getS2CellId(s2Lvl14) should equal(s2CellIdLvl14)
    entity.getS2CellId(s2Lvl15) should equal(s2CellIdLvl15)
  }

  "S2CellTokenGeoEntity.getS2CellId" should "return a the right cell" in
  {
    val entity =
      S2CellTokenGeoEntity(s2CellTokenLvl14)

    entity.getS2CellId(s2Lvl14) should equal(s2CellIdLvl14)
  }

}
