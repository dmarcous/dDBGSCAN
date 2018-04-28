package com.github.dmarcous.ddbgscan.api

import com.github.dmarcous.ddbgscan.core.CoreConfig._
import com.github.dmarcous.ddbgscan.core.{AlgorithmParameters, IOConfig}
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class CLIRunnerTest extends FlatSpec{
  val epsilon = 10.0
  val minPts = 5
  val neighborhoodPartitioningLvl = 14
  val isNeighbourInstances = DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION
  val parameters = AlgorithmParameters(
    epsilon,
    minPts,
    neighborhoodPartitioningLvl,
    isNeighbourInstances
  )
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
  val conf =
    RuntimeConfig(
      ioConfig,
      parameters
    )

  "parseArgs" should "parse all args correctly" in
  {
    val parsedConf = CLIRunner.parseArgs(Array(
      "--inputFilePath",inputPath,"--outputFolderPath",outputFolderPath,
      "--positionFieldId",positionId.toString,"--positionFieldLon",positionLon.toString,"--positionFieldLat",positionLat.toString,
      "--inputFieldDelimiter",delimiter,
      "--epsilon",epsilon.toString,"--minPts",minPts.toString,
      "--neighborhoodPartitioningLvl",neighborhoodPartitioningLvl.toString,
      "--isNeighbourInstances_function_code",DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION_CODE.toString
    )

    )

    conf should equal(parsedConf)

  }

}
