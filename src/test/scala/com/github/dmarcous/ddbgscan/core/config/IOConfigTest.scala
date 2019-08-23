package com.github.dmarcous.ddbgscan.core.config

import com.github.dmarcous.ddbgscan.core.config.CoreConfig._
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class IOConfigTest extends FlatSpec{
  val inputPath = "s3://my.bucket/ddbgscan/inputs/input.csv"
  val outputFolderPath = "s3://my.bucket/ddbgscan/outputs/"
  val positionId = NO_UNIQUE_ID_FIELD
  val positionLon = DEFAULT_LONGITUDE_POSITION_FIELD_NUMBER
  val positionLat = DEFAULT_LATITUDE_POSITION_FIELD_NUMBER
  val delimiter = DEFAULT_GEO_FILE_DELIMITER
  val numPartitions = DEFAULT_NUM_PARTITIONS

  "Full constructor" should "return a valid object" in
  {
    val params =
      IOConfig(
        inputPath,
        outputFolderPath,
        positionId,
        positionLon,
        positionLat,
        delimiter,
        numPartitions
      )

    params.inputPath should equal (inputPath)
    params.outputFolderPath should equal (outputFolderPath)
    params.positionId should equal (positionId)
    params.positionLon should equal (positionLon)
    params.positionLat should equal (positionLat)
    params.inputDelimiter should equal (delimiter)
    params.numPartitions should equal (numPartitions)

  }

  "Short constructor" should "return a valid object with defaults" in
  {
    val params =
      IOConfig(
        inputPath,
        outputFolderPath
      )

    params.inputPath should equal (inputPath)
    params.outputFolderPath should equal (outputFolderPath)
    params.positionId should equal (NO_UNIQUE_ID_FIELD)
    params.positionLon should equal (DEFAULT_LONGITUDE_POSITION_FIELD_NUMBER)
    params.positionLat should equal (DEFAULT_LATITUDE_POSITION_FIELD_NUMBER)
    params.inputDelimiter should equal (DEFAULT_GEO_FILE_DELIMITER)
    params.numPartitions should equal (DEFAULT_NUM_PARTITIONS)

  }

}
