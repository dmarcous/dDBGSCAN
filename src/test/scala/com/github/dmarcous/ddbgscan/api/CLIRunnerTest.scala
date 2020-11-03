package com.github.dmarcous.ddbgscan.api

import java.io.File

import com.github.dmarcous.ddbgscan.core.config.CoreConfig._
import com.github.dmarcous.ddbgscan.core.config.{AlgorithmParameters, IOConfig}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class CLIRunnerTest extends FlatSpec{
  val epsilon = 100.0
  val minPts = 3
  val neighborhoodPartitioningLvl = 15
  val isNeighbourInstances = DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION
  val parameters = AlgorithmParameters(
    epsilon,
    minPts,
    GEO_PARTITIONING_STRATEGY,
    MISSING_GEO_DECIMAL_SENSITIVITY_LVL,
    neighborhoodPartitioningLvl,
    DEFAULT_NUM_PARTITIONS,
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

  val args =
    Array(
      "--inputFilePath",inputPath,"--outputFolderPath",outputFolderPath,
      "--positionFieldId",positionId.toString,"--positionFieldLon",positionLon.toString,"--positionFieldLat",positionLat.toString,
      "--inputFieldDelimiter",delimiter,
      "--numPartitions",numPartitions.toString,
      "--epsilon",epsilon.toString,"--minPts",minPts.toString,
      "--neighborhoodPartitioningLvl",neighborhoodPartitioningLvl.toString
    )

  "parseArgs" should "parse all args correctly" in
  {
    println("args")
    println(args.foreach(println))
    val parsedConf = CLIRunner.parseArgs(args)
    conf should equal(parsedConf)
  }

  "main" should "run full dDBGSCAN pipeline from CLI params" in
  {
    // Create outside test so test will get it from here
    val spark =
      SparkSession
        .builder()
        .master("local")
        .appName("CLIRunnerTest")
        .getOrCreate()

    // Clean output before run
    FileUtils.deleteQuietly(new File(outputFolderPath))

    // Run algorithm
    CLIRunner.main(args)
  }

}
