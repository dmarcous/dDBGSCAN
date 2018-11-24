package com.github.dmarcous.ddbgscan.core.algo

import com.github.dmarcous.ddbgscan.core.config.CoreConfig._
import com.github.dmarcous.ddbgscan.core.config.{AlgorithmParameters, IOConfig}
import com.github.dmarcous.ddbgscan.core.preprocessing.GeoPropertiesExtractor
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class dDBGSCANTest extends FlatSpec{

  val spark =
    SparkSession
      .builder()
      .master("local")
      .appName("dDBGSCANTest")
      .getOrCreate()
  import spark.implicits._

  val S2_LVL = 15
  val epsilon= 100.0

  val complexLonLatDelimitedGeoData =
    spark.createDataset(
      Seq(
        // Pair cluster
        "34.778023,32.073889,6,7", //Address - Tsemach Garden
        "34.778137,32.074229,10,11", //Address - Dizzy21-25
        // NOISE
        "34.775628,32.074032,5,5", //Address - Voodoo
        //Triplet cluster
        "34.777112,32.0718015,8,9", //Address - Haimvelish1-7
        "34.777547,32.072729,11,2", //Address - BenTsiyon22-28
        "34.777558,32.072565,3,4" //Address - Warburg9-3
      ))

  val inputPath = "s3://my.bucket/ddbgscan/inputs/input.csv"
  val outputFolderPath = "s3://my.bucket/ddbgscan/outputs/"
  val positionId = NO_UNIQUE_ID_FIELD
  val positionLon = DEFAULT_LONGITUDE_POSITION_FIELD_NUMBER
  val positionLat = DEFAULT_LATITUDE_POSITION_FIELD_NUMBER
  val delimiter = DEFAULT_GEO_FILE_DELIMITER
  val ioConfig = IOConfig(
    inputPath,
    outputFolderPath,
    positionId,
    positionLon,
    positionLat,
    delimiter
  )

  val minPts = 2
  val params =
    AlgorithmParameters(
      epsilon,
      minPts,
      S2_LVL,
      DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION
    )

  val VOODO_CELL_KEY = 1521455263322734592L

  val clusteringDataset =
    GeoPropertiesExtractor.fromLonLatDelimitedFile(
      spark, complexLonLatDelimitedGeoData, S2_LVL, ioConfig
    )

  "run" should " cluster given instances " in
  {

  }

}
