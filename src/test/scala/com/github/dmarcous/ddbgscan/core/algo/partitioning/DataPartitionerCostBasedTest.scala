package com.github.dmarcous.ddbgscan.core.algo.partitioning

import com.github.dmarcous.ddbgscan.core.config.CoreConfig.{DEFAULT_GEO_FILE_DELIMITER, DEFAULT_LATITUDE_POSITION_FIELD_NUMBER, DEFAULT_LONGITUDE_POSITION_FIELD_NUMBER, NO_UNIQUE_ID_FIELD}
import com.github.dmarcous.ddbgscan.core.config.IOConfig
import com.github.dmarcous.ddbgscan.core.preprocessing.GeoPropertiesExtractor
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class DataPartitionerCostBasedTest extends FlatSpec{

  val spark =
    SparkSession
      .builder()
      .master("local")
      .appName("DataPartitionerCostBasedTest")
      .getOrCreate()
  import spark.implicits._

  val S2_LVL = 15
  val maxPointsPerPartition=3
  val maxPointsPerPartition_minimal=1
  val epsilon_minimal = 1.0
  val epsilon_partly_outside_range = 20.0
  val GEO_DATA_KEYS = Array(1521455269765185536L, 1521455263322734592L)
  val GEO_DATA_VALUES =
    Array(
      Array(1.0, 2.0, 3.0, 4.0, 5.0),
      Array(6.0, 7.0, 8.0, 9.0, 10.0),
      Array(1.0, 2.0, 3.0, 4.0, 5.0),
      Array(6.0, 7.0, 8.0, 9.0, 10.0)
    )

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
  val numPartitions = 1
  val ioConfig = IOConfig(
    inputPath,
    outputFolderPath,
    positionId,
    positionLon,
    positionLat,
    delimiter,
    numPartitions
  )

  val complexDataset =
    GeoPropertiesExtractor.fromLonLatDelimitedFile(
      spark, complexLonLatDelimitedGeoData, S2_LVL, ioConfig
    )

  "partitionData" should "keep close together points in the same partition" in
  {
    val partitionedData = DataPartitionerCostBased.partitionData(spark, complexDataset, epsilon_partly_outside_range, maxPointsPerPartition)

    import spark.implicits._
    val collectedPartitionedData = partitionedData.flatMap{case(key, vals) => (vals.map(instance => (key,instance)))}.collect()

    collectedPartitionedData.foreach(println)
    collectedPartitionedData.map(_._1).distinct.size should equal(2)
  }
  it should "keep a single point per partition if asked" in
  {
    val partitionedData = DataPartitionerCostBased.partitionData(spark, complexDataset, epsilon_minimal, maxPointsPerPartition_minimal)

    import spark.implicits._
    val collectedPartitionedData = partitionedData.flatMap{case(key, vals) => (vals.map(instance => (key,instance)))}.collect()

    collectedPartitionedData.foreach(println)
    collectedPartitionedData.map(_._1).distinct.size should equal(6)
  }

}
