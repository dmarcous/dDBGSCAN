package com.github.dmarcous.ddbgscan.api

import java.io.File

import com.github.dmarcous.ddbgscan.core.config.CoreConfig._
import com.github.dmarcous.ddbgscan.core.config.{AlgorithmParameters, IOConfig}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class dDBGSCANRunnerTest extends FlatSpec{
  val epsilon = 100.0
  val minPts = 3
  val neighborhoodPartitioningLvl = 15
  val isNeighbourInstances = DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION
  val parameters = AlgorithmParameters(
    epsilon,
    minPts,
    neighborhoodPartitioningLvl,
    isNeighbourInstances
  )
  val inputPath = "./src/test/resources/complexLonLatDelimitedGeoData.csv"
  val outputFolderPath = "/tmp/dDBGSCAN/"
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
  val conf =
    RuntimeConfig(
      ioConfig,
      parameters
    )
  
  "run" should "run full dDBGSCAN pipeline " in
  {
    // Create outside test so test will get it from here
    val spark =
      SparkSession
        .builder()
        .master("local")
        .appName("dDBGSCANRunnerTest")
        .getOrCreate()

    // Clean output before run
    FileUtils.deleteQuietly(new File(outputFolderPath))

    // Run algorithm
    dDBGSCANRunner.run(spark, conf)
  }

}
