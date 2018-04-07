package com.github.dmarcous.ddbgscan.core

import com.github.dmarcous.ddbgscan.core.CoreConfig.{DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION, MISSING_NEIGHBORHOOD_LVL}
import org.apache.spark.ml.linalg.Vector

case class AlgorithmParameters(
    epsilon : Double,
    minPts : Int,
    neighborhoodPartitioningLvl : Int = MISSING_NEIGHBORHOOD_LVL,
    isNeighbourInstances : (Vector, Vector) => Boolean = DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION
)
{

}
