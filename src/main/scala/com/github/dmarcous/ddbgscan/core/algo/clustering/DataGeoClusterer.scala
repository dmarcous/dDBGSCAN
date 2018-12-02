package com.github.dmarcous.ddbgscan.core.algo.clustering

import com.github.davidmoten.rtree.geometry.Point
import com.github.davidmoten.rtree.{Entry, RTree}
import com.github.dmarcous.ddbgscan.core.config.AlgorithmParameters
import com.github.dmarcous.ddbgscan.core.config.CoreConfig.ClusteringInstanceStatusValue.{BORDER, CORE, NOISE}
import com.github.dmarcous.ddbgscan.core.config.CoreConfig.UNKNOWN_CLUSTER
import com.github.dmarcous.ddbgscan.model.ClusteringInstance
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable.Queue

object DataGeoClusterer {

  def clusterGeoData(@transient spark: SparkSession,
                     data: Dataset[(Long, List[ClusteringInstance])],
                     parameters: AlgorithmParameters
                    ): Dataset[(Long, List[ClusteringInstance])]  =
  {
    import spark.implicits._

    val clusteredData =
      data.map{case(key, vals) => (key,DataGeoClusterer.clusterLocalGeoData(key, vals, parameters))}

    clusteredData
  }

  def clusterLocalGeoData(
                     geoKey: Long,
                     instances: List[ClusteringInstance],
                     parameters: AlgorithmParameters
         ): List[ClusteringInstance] =
  {
    var currentCluster = UNKNOWN_CLUSTER

    val searchTree = PointSearchUtilities.buildPointGeometrySearchTree(instances)
    searchTree.entries().toBlocking().toIterable.asScala.foreach{
      // point - same p as in DBSCAN original implementation
      case (p) => {
        // Go over all unvisited points
        if (!p.value().isVisited)
        {
          // Mark current point as visited
          p.value().isVisited = true

          // Find neighbours
          val neighbours = NeighbourUtilities.getNeighbours(p,
            parameters.epsilon, searchTree, parameters.isNeighbourInstances)

          // Noise - not enough neighbours
          if (neighbours.size + 1 < parameters.minPts)
          {
            p.value().instanceStatus = NOISE.value
          }
          // Current point is part of a cluster
          else
          {
            currentCluster = p.value().recordId // Use recordId to have clusters globally unique

            // Create a cluster for point p and mark it as a core point
            p.value().cluster = currentCluster
            p.value().instanceStatus = CORE.value

            // Iterate over neighbours (in DBSCAN article called "Expand the cluster)
            expandCluster(neighbours, searchTree, currentCluster, parameters)
          }
        }// traverse unvisited points
      }
    }// traverse points

    // Return all clustered instances (stored in the search tree)
    PointSearchUtilities.getClusteringInstancesFromSearchTree(searchTree)
  }

  // Expand the cluster - go over cluster neighbourhood and assign status (in / out current cluster)
  def expandCluster(neighbours: List[Entry[ClusteringInstance, Point]],
                    searchTree: RTree[ClusteringInstance, Point],
                    currentCluster: Long,
                    parameters: AlgorithmParameters): Unit =
  {
    // Put neighbourhood in a mutable queue to be able to traverse and expand
    // Each element in the Queue is a neighbourhood list
    val nbhdP = Queue(neighbours)

    // Traverse neighbourhood until finished
    while (!nbhdP.isEmpty)
    {
      // Go over all neighbours (q) in the current queued neighbourhood
      nbhdP.dequeue().foreach{case(q) => {
        // Neighbour is yet to be visited
        if(!q.value().isVisited)
        {
          // Mark current point as visited and assign to current cluster
          q.value().isVisited = true
          q.value().cluster = currentCluster

          // Get neighbour's neighbourhood
          // Find neighbours
          val nbhdQ = NeighbourUtilities.getNeighbours(q,
            parameters.epsilon, searchTree, parameters.isNeighbourInstances)
          // Check if q has enough neighbours to be part of a cluster
          if (nbhdQ.size + 1 >= parameters.minPts)
          {
            // Set as a core point
            q.value().instanceStatus = CORE.value
            // Add neighbour's neighbourhood to the queue to be tested as part of the cluster
            nbhdP.enqueue(nbhdQ)
          }
          // assign to border (not noise, because q is already a neighbour of p if we got here)
          else
          {
            q.value().instanceStatus = BORDER.value
          }
        }
        // Neighbour already visited and marked as noise
        else if (q.value().instanceStatus == NOISE.value)
        {
          // I'm not directly reachable to minPts , but together with my neighbour's neighbours I am.
          // Change previous assignment to border - as we now know that it is part of our cluster (q is reachable from p)
          q.value().cluster = currentCluster
          q.value().instanceStatus = BORDER.value
        }
      }}
    }
  }
}
