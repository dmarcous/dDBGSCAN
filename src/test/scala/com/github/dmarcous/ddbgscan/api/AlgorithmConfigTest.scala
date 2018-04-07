package com.github.dmarcous.ddbgscan.api

import com.github.dmarcous.ddbgscan.core.AlgorithmParameters
import com.github.dmarcous.ddbgscan.core.CoreConfig.{DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION, MISSING_NEIGHBORHOOD_LVL}
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class AlgorithmConfigTest extends FlatSpec{
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

  "Full constructor" should "return a valid object" in
  {
    val conf =
      AlgorithmConfig(
        inputPath,
        outputFolderPath,
        parameters
      )

    conf.inputPath should equal(inputPath)
    conf.outputFolderPath should equal(outputFolderPath)
    conf.parameters should equal(parameters)

  }

  "Short constructor" should "return a valid object with defaults" in
  {
    val conf =
      AlgorithmConfig(
        inputPath,
        outputFolderPath,
        AlgorithmParameters(
          epsilon,
          minPts
        )
      )

    conf.inputPath should equal(inputPath)
    conf.outputFolderPath should equal(outputFolderPath)
    conf.parameters.epsilon should equal(epsilon)
    conf.parameters.minPts should equal(minPts)
    conf.parameters.neighborhoodPartitioningLvl should equal(MISSING_NEIGHBORHOOD_LVL)
    conf.parameters.isNeighbourInstances should equal(DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION)

  }

}
