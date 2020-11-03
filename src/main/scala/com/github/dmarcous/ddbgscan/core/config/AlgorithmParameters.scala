package com.github.dmarcous.ddbgscan.core.config

import CoreConfig.{DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION, DEFAULT_NUM_PARTITIONS, MISSING_NEIGHBORHOOD_LVL, SMALLEST_CELL_AREA_EPSILON_MULTIPLIER, SUPPORTED_PARTITIONING_STRATEGIES, MISSING_GEO_DECIMAL_SENSITIVITY_LVL}
import com.github.dmarcous.ddbgscan.core.algo.partitioning.DataPartitionerS2
import com.github.dmarcous.s2utils.converters.UnitConverters
import com.github.dmarcous.s2utils.s2.S2Utilities
import com.google.common.geometry.S2Projections
import org.apache.spark.ml.linalg.Vector

case class AlgorithmParameters(
    epsilon : Double,
    minPts : Int,
    partitioningStrategy : String = SUPPORTED_PARTITIONING_STRATEGIES.head,
    geoDecimalPlacesSensitivity : Int = MISSING_GEO_DECIMAL_SENSITIVITY_LVL,
    var neighborhoodPartitioningLvl : Int = MISSING_NEIGHBORHOOD_LVL,
    maxPointsPerPartition : Int = DEFAULT_NUM_PARTITIONS,
    isNeighbourInstances : (Vector, Vector) => Boolean = DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION
)
{
  neighborhoodPartitioningLvl =
    this.adjustNeighborhoodPartitioningLvl(
      neighborhoodPartitioningLvl, epsilon)

  private def adjustNeighborhoodPartitioningLvl(inputNeighborhoodPartitioningLvl: Int, epsilon: Double): Int = {
    val fillLevel = getNeighborhoodPartitioningLvlIfMissing(inputNeighborhoodPartitioningLvl, epsilon)
    getMinimalNeighborhoodPartitioningLvlIfTooGranular(fillLevel, epsilon)
  }

  private def getNeighborhoodPartitioningLvlIfMissing(inputNeighborhoodPartitioningLvl: Int, epsilon: Double): Int = {
    if (inputNeighborhoodPartitioningLvl == MISSING_NEIGHBORHOOD_LVL) {
      return getDefaultNeighborhoodPartitioningLvl(epsilon)
    }
    else {
      return inputNeighborhoodPartitioningLvl
    }
  }

  def getDefaultNeighborhoodPartitioningLvl(epsilon: Double): Int = {
    S2Utilities.getLevelForArea(
      SMALLEST_CELL_AREA_EPSILON_MULTIPLIER * Math.pow(epsilon,2))
  }

  private def getMinimalNeighborhoodPartitioningLvlIfTooGranular(inputNeighborhoodPartitioningLvl: Int, epsilon: Double): Int = {
    val minimalNeighborhoodPartitioningLvl = getMinimalNeighborhoodPartitioningLvl(epsilon)
    if (minimalNeighborhoodPartitioningLvl < inputNeighborhoodPartitioningLvl) {
      return minimalNeighborhoodPartitioningLvl
    }
    else {
      return inputNeighborhoodPartitioningLvl
    }
  }

  def getMinimalNeighborhoodPartitioningLvl(epsilon: Double): Int = {
    val minArea = DataPartitionerS2.getMinimalLookupArea(epsilon)
    S2Projections.MIN_AREA.getMaxLevel(minArea)
  }
}

