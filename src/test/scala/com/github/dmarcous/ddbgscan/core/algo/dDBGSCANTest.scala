package com.github.dmarcous.ddbgscan.core.algo

import com.github.dmarcous.ddbgscan.core.config.CoreConfig.ClusteringInstanceStatusValue.{BORDER, CORE, NOISE}
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

  spark.sparkContext.setCheckpointDir("/tmp")

  val S2_LVL = 15
  val epsilon= 100.0

  val complexLonLatDelimitedGeoData =
    spark.createDataset(
      Seq(
        // Pair cluster
        "11,34.778023,32.073889,6,7", //Address - Tsemach Garden
        "12,34.778137,32.074229,10,11", //Address - Dizzy21-25
        "7,34.778982,32.074499,13,14", //Address - Shmaryahu Levin St 3
        "8,34.778998,32.075054,15,16", //Address - Shmaryahu Levin St 9,
        "9,34.779212,32.075217,17,18", //Address - Shmaryahu Levin St 14,
        "10,34.779861,32.075049,19,20", //Address - Sderot Chen 11,
        "1,34.779046,32.073903,21,22", //Adress - Tarsat
        "2,34.779765,32.073458,23,24", //Adress - HabimaSquare
        // NOISE
        "3,34.775628,32.074032,5,5", //Address - Voodoo
        //Triplet cluster
        "4,34.777112,32.0718015,8,9", //Address - Haimvelish1-7
        "5,34.777547,32.072729,11,2", //Address - BenTsiyon22-28
        "6,34.777558,32.072565,3,4" //Address - Warburg9-3
      ))

  val inputPath = "s3://my.bucket/ddbgscan/inputs/input.csv"
  val outputFolderPath = "s3://my.bucket/ddbgscan/outputs/"
  val positionId = 0
  val positionLon = DEFAULT_LONGITUDE_POSITION_FIELD_NUMBER+1
  val positionLat = DEFAULT_LATITUDE_POSITION_FIELD_NUMBER+1
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

  val minPts = 3
  val params =
    AlgorithmParameters(
      epsilon,
      minPts,
      GEO_PARTITIONING_STRATEGY,
      S2_LVL,
      DEFAULT_NUM_PARTITIONS,
      DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION
    )

  val clusteringDataset =
    GeoPropertiesExtractor.fromLonLatDelimitedFile(
      spark, complexLonLatDelimitedGeoData, S2_LVL, ioConfig
    )

  "run" should " cluster given instances " in
  {
    val globallyClusteredData =
      dDBGSCAN.run(spark, clusteringDataset, params, DEFAULT_DEBUG)

    // Returns ClusteredInstance
    val clusteringResults = globallyClusteredData.collect
    clusteringResults.foreach(println)
    clusteringResults.size should equal(12)
    clusteringResults.map(_.cluster).distinct.size should equal (3)
    clusteringResults.filter(_.instanceStatus == CORE.value).size should equal (8)
    clusteringResults.filter(_.instanceStatus == BORDER.value).size should equal (3)
    clusteringResults.filter(_.instanceStatus == NOISE.value).size should equal (1)
  }

}
