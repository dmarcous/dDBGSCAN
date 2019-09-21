package com.github.dmarcous.ddbgscan.core.config

import com.github.dmarcous.ddbgscan.core.config.CoreConfig.{DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION, DEFAULT_NUM_PARTITIONS, GEO_PARTITIONING_STRATEGY, MISSING_NEIGHBORHOOD_LVL}
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class AlgorithmParametersTest extends FlatSpec{
  val epsilon = 10.0
  val minPts = 5
  val neighborhoodPartitioningLvl = 14
  val FilledNeighborhoodPartitioningLvl = 18
  val isNeighbourInstances = DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION

  "getMinimalNeighborhoodPartitioningLvl" should "get minimal allowed level for partitioning" in
  {
    val eps = 300.0
    val minPts = 1
    AlgorithmParameters(eps,minPts).getMinimalNeighborhoodPartitioningLvl(eps) should equal (neighborhoodPartitioningLvl)
  }

  "Full constructor" should "return a valid object" in
  {
    val params =
      AlgorithmParameters(
        epsilon,
        minPts,
        GEO_PARTITIONING_STRATEGY,
        neighborhoodPartitioningLvl,
        DEFAULT_NUM_PARTITIONS,
        isNeighbourInstances
      )

    params.epsilon should equal(epsilon)
    params.minPts should equal(minPts)
    params.neighborhoodPartitioningLvl should equal(neighborhoodPartitioningLvl)
    params.isNeighbourInstances should equal(isNeighbourInstances)

  }

  "Full constructor with missing level flag" should "return a valid filled object" in
    {
      val params =
        AlgorithmParameters(
          epsilon,
          minPts,
          GEO_PARTITIONING_STRATEGY,
          MISSING_NEIGHBORHOOD_LVL,
          DEFAULT_NUM_PARTITIONS,
          isNeighbourInstances
        )

      params.epsilon should equal(epsilon)
      params.minPts should equal(minPts)
      params.neighborhoodPartitioningLvl should equal(FilledNeighborhoodPartitioningLvl)
      params.isNeighbourInstances should equal(isNeighbourInstances)

    }

  "Short constructor" should "return a valid object with defaults" in
  {
    val params =
      AlgorithmParameters(
        epsilon,
        minPts
      )

    params.epsilon should equal(epsilon)
    params.minPts should equal(minPts)
    params.neighborhoodPartitioningLvl should equal(FilledNeighborhoodPartitioningLvl)
    params.isNeighbourInstances should equal(DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION)

  }

}
