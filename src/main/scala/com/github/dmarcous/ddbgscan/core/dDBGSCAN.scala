package com.github.dmarcous.ddbgscan.core

import CoreConfig.MISSING_NEIGHBORHOOD_LVL
import CoreConfig.DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION
import CoreConfig.SMALLEST_CELL_AREA_EPSILON_MULTIPLIER
import com.github.dmarcous.ddbgscan.model.{ClusteringInstance, KeyGeoEntity}
import com.github.dmarcous.s2utils.s2.S2Utilities
import org.apache.spark.sql.{Dataset, SparkSession}

object dDBGSCAN {

  def run(@transient spark: SparkSession,
          data: Dataset[(KeyGeoEntity, ClusteringInstance)],
          epsilon: Double, minPts: Int,
          inputNeighborhoodPartitioningLvl: Int = MISSING_NEIGHBORHOOD_LVL,
          isNeighbourInstances: (Any, Any) => Boolean = DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION): Dataset[String] =
  {
    // Init internal parameters
    val neighborhoodPartitioningLvl =
      getNeighborhoodPartitioningLvlIfMissing(
        inputNeighborhoodPartitioningLvl, epsilon)

    // Partition data
    val partitionedData = null

    // Perform local clustering
    val locallyClusteredData = null

    // Perform global merging
    val globallyClusteredData = null

    globallyClusteredData
  }

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
