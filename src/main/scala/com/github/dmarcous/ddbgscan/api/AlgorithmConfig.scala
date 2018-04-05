package com.github.dmarcous.ddbgscan.api

import com.github.dmarcous.ddbgscan.core.CoreConfig.MISSING_NEIGHBORHOOD_LVL
import com.github.dmarcous.ddbgscan.core.CoreConfig.DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION

case class AlgorithmConfig(
    inputPath : String,
    outputFolderPath : String,
    epsilon : Double,
    minPts : Int,
    neighborhoodPartitioningLvl : Int = MISSING_NEIGHBORHOOD_LVL,
    isNeighbourInstances : (Any, Any) => Boolean = DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION
)
{

}
