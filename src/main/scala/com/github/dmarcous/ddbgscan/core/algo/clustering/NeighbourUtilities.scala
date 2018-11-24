package com.github.dmarcous.ddbgscan.core.algo.clustering

import com.github.davidmoten.rtree.geometry.Point
import com.github.davidmoten.rtree.{Entry, RTree}
import com.github.dmarcous.ddbgscan.core.config.CoreConfig.DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION
import com.github.dmarcous.ddbgscan.model.ClusteringInstance
import org.apache.spark.ml.linalg.Vector

object NeighbourUtilities {

  // Get neighbours based on similarity based on geo search tree & user defined function
  def getNeighbours(p: Entry[ClusteringInstance, Point],
                    epsilon: Double,
                    searchTree: RTree[ClusteringInstance, Point],
                    userDefinedFilteringFunction : (Vector, Vector) => Boolean
                   ) : List[Entry[ClusteringInstance, Point]] =
  {
    // Get geo reachable neighbour list
    val geoNeighbours =
      PointSearchUtilities.getGeoDensityReachablePointsFromSearchTree(searchTree, p.geometry(), epsilon)
    // Remove self
    val geoNeighboursWithoutSelf = geoNeighbours.filter(_ != p)
    // Get final list after user defined similarity function
    val neighbours = applyUserDefinedFiltering(p, geoNeighboursWithoutSelf, userDefinedFilteringFunction)

    neighbours
  }

  // Apply function if exists (default is return all = no function)
  def applyUserDefinedFiltering(p: Entry[ClusteringInstance, Point],
                                neighbours: List[Entry[ClusteringInstance, Point]],
                                userDefinedFilteringFunction : (Vector, Vector) => Boolean
                               ) : List[Entry[ClusteringInstance, Point]] =
  {
    if (userDefinedFilteringFunction == DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION)
    {
      return neighbours
    }
    else
    {
      return neighbours.filter(neighbour =>
        userDefinedFilteringFunction(p.value().features,neighbour.value().features))
    }
  }
}
