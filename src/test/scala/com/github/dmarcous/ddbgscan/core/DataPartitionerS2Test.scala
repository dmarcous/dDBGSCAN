package com.github.dmarcous.ddbgscan.core

import com.github.dmarcous.ddbgscan.core.CoreConfig.{DEFAULT_GEO_FILE_DELIMITER, DEFAULT_LATITUDE_POSITION_FIELD_NUMBER, DEFAULT_LONGITUDE_POSITION_FIELD_NUMBER, NO_UNIQUE_ID_FIELD}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class DataPartitionerS2Test extends FlatSpec{

  val spark =
    SparkSession
      .builder()
      .master("local")
      .appName("DataPartitionerS2Test")
      .getOrCreate()
  import spark.implicits._

  val S2_LVL = 15
  val epsilon_in_range = 10.0
  val epsilon_outside_range = 100.0
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

  val clusteringDataset =
    GeoPropertiesExtractor.fromLonLatDelimitedFile(
      spark, defaultLonLatDelimitedGeoData, S2_LVL, ioConfig
    )


  "fromLonLatDelimitedFile" should "transform a geo delimited dataset with default properties to a geo clustering input dataset" in
  {
    import spark.implicits._
    val partitionedData = DataPartitionerS2.partitionData(spark, clusteringDataset, epsilon_in_range, S2_LVL)

    partitionedData.flatMapGroups((key, vals) => (vals.toList)).collect().foreach(println)

    partitionedData.keys.count() should equal(2)
    partitionedData.keys.collect() should equal (GEO_DATA_KEYS)
    partitionedData.flatMapGroups((key, vals) => vals.map(_.features.toArray).toList).collect() should equal (GEO_DATA_VALUES)
  }

  "getDensityReachableCells" should "be tested" in
  {

  }

}
