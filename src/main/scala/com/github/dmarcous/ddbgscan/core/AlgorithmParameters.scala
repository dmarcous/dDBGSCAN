package com.github.dmarcous.ddbgscan.core

import com.github.dmarcous.ddbgscan.core.CoreConfig.{DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION, MISSING_NEIGHBORHOOD_LVL, SMALLEST_CELL_AREA_EPSILON_MULTIPLIER}
import com.github.dmarcous.s2utils.s2.S2Utilities
import org.apache.spark.ml.linalg.Vector

case class AlgorithmParameters(
    epsilon : Double,
    minPts : Int,
    var neighborhoodPartitioningLvl : Int = MISSING_NEIGHBORHOOD_LVL,
    isNeighbourInstances : (Vector, Vector) => Boolean = DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION
)
{

  neighborhoodPartitioningLvl =
    this.getNeighborhoodPartitioningLvlIfMissing(
      neighborhoodPartitioningLvl, epsilon)

  private def getNeighborhoodPartitioningLvlIfMissing(inputNeighborhoodPartitioningLvl: Int, epsilon: Double): Int = {
    if (inputNeighborhoodPartitioningLvl == MISSING_NEIGHBORHOOD_LVL) {
      return getDefaultNeighborhoodPartitioningLvl(epsilon)
    }
    else {
      return inputNeighborhoodPartitioningLvl
    }
  }

  private def getDefaultNeighborhoodPartitioningLvl(epsilon: Double): Int = {
    S2Utilities.getLevelForArea(
      SMALLEST_CELL_AREA_EPSILON_MULTIPLIER * Math.pow(epsilon,2))
  }
}

