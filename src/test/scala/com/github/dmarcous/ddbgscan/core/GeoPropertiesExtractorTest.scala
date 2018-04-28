package com.github.dmarcous.ddbgscan.core

import com.github.dmarcous.ddbgscan.core.CoreConfig.{DEFAULT_GEO_FILE_DELIMITER, DEFAULT_LATITUDE_POSITION_FIELD_NUMBER, DEFAULT_LONGITUDE_POSITION_FIELD_NUMBER, NO_UNIQUE_ID_FIELD}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class GeoPropertiesExtractorTest extends FlatSpec{

  val spark =
    SparkSession
      .builder()
      .master("local")
      .appName("GeoPropertiesExtractorTest")
      .getOrCreate()
  import spark.implicits._

  val S2_LVL = 15
  val GEO_DATA_KEYS = Array(1521455259027767296L,1521455265470218240L)
  val GEO_DATA_VALUES =
    Array(
      Array(1.0,2.0,3.0,4.0,5.0),
      Array(6.0,7.0,8.0,9.0,10.0)
    )

  val defaultLonLatDelimitedGeoData =
    spark.createDataset(
      Seq(
        "34.777112,32.0718015,1,2,3,4,5",
        "34.7747174,32.0774609,6,7,8,9,10"
      ))

  val defaultLonLatGeoDataframe =
    spark.createDataFrame(
      spark.sparkContext.makeRDD(
        List(
          Row(34.777112,32.0718015,1,2,3,4,5),
          Row(34.7747174,32.0774609,6,7,8,9,10)
        )),
      StructType(List(
        StructField("lon", DoubleType, false),
        StructField("lat", DoubleType, false),
        StructField("ft1", IntegerType, false),
        StructField("ft2", IntegerType, false),
        StructField("ft3", IntegerType, false),
        StructField("ft4", IntegerType, false),
        StructField("ft5", IntegerType, false))
      )
    )

  val customLonLatDelimitedGeoData =
    spark.createDataset(
      Seq(
        "1,34.777112,32.0718015,1,2,3,4,5",
        "2,34.7747174,32.0774609,6,7,8,9,10"
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
  val customPositionId = 0
  val customPositionLon = 1
  val customPositionLat = 2
  val customIOConfig = IOConfig(
    inputPath,
    outputFolderPath,
    customPositionId,
    customPositionLon,
    customPositionLat,
    delimiter
  )


  "fromLonLatDelimitedFile" should "transform a geo delimited dataset with default properties to a geo clustering input dataset" in
  {
    val clusteringDataset =
      GeoPropertiesExtractor.fromLonLatDelimitedFile(
        spark, defaultLonLatDelimitedGeoData, S2_LVL, ioConfig
      )

    clusteringDataset.collect().foreach(println)

    clusteringDataset.count() should equal(2)
    clusteringDataset.map(_._1.s2CellId).collect() should equal (GEO_DATA_KEYS)
    clusteringDataset.map(_._2.features.toArray).collect() should equal (GEO_DATA_VALUES)
  }
  it should "transform a geo delimited dataset with custom properties to a geo clustering input dataset" in
  {
    val clusteringDataset =
      GeoPropertiesExtractor.fromLonLatDelimitedFile(
        spark, customLonLatDelimitedGeoData, S2_LVL, customIOConfig
      )

    clusteringDataset.collect().foreach(println)

    clusteringDataset.count() should equal(2)
    clusteringDataset.map(_._1.s2CellId).collect() should equal (GEO_DATA_KEYS)
    clusteringDataset.map(_._2.features.toArray).collect() should equal (GEO_DATA_VALUES)
  }

  "fromLonLatDataFrame" should "transform a geo delimited dataset to a geo clustering input dataset" in
  {
    val clusteringDataset =
      GeoPropertiesExtractor.fromLonLatDataFrame(
        spark, defaultLonLatGeoDataframe, S2_LVL, ioConfig
      )

    clusteringDataset.collect().foreach(println)

    clusteringDataset.count() should equal(2)
    clusteringDataset.map(_._1.s2CellId).collect() should equal (GEO_DATA_KEYS)
    clusteringDataset.map(_._2.features.toArray).collect() should equal (GEO_DATA_VALUES)

  }

}
