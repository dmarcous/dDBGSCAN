package com.github.dmarcous.ddbgscan.api

import com.github.dmarcous.ddbgscan.core.config.CoreConfig._
import com.github.dmarcous.ddbgscan.core.config.{AlgorithmParameters, IOConfig}
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class RuntimeConfigTest extends FlatSpec{
  val epsilon = 10.0
  val minPts = 5
  val neighborhoodPartitioningLvl = 14
  val completedNeighborhoodPartitioningLvl = 18
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

  "Full constructor" should "return a valid object" in
  {
    val conf =
      RuntimeConfig(
        ioConfig,
        parameters
      )

    conf.ioConfig should equal(ioConfig)
    conf.parameters should equal(parameters)

  }

  "Short constructor" should "return a valid object with defaults" in
  {
    val conf =
      RuntimeConfig(
        IOConfig(
          inputPath,
          outputFolderPath
        ),
        AlgorithmParameters(
          epsilon,
          minPts
        )
      )

    conf.ioConfig.inputPath should equal (inputPath)
    conf.ioConfig.outputFolderPath should equal (outputFolderPath)
    conf.ioConfig.positionId should equal (positionId)
    conf.ioConfig.positionLon should equal (positionLon)
    conf.ioConfig.positionLat should equal (positionLat)
    conf.ioConfig.inputDelimiter should equal (delimiter)
    conf.parameters.epsilon should equal(epsilon)
    conf.parameters.minPts should equal(minPts)
    conf.parameters.neighborhoodPartitioningLvl should equal(completedNeighborhoodPartitioningLvl)
    conf.parameters.isNeighbourInstances should equal(DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION)

  }

}
