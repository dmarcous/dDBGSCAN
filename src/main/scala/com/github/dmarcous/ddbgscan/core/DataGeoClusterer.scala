package com.github.dmarcous.ddbgscan.core

import com.github.davidmoten.rtree.{Entry, RTree}
import com.github.davidmoten.rtree.geometry.Point
import com.github.dmarcous.ddbgscan.core.CoreConfig.ClusteringInstanceStatusValue.NOISE
import com.github.dmarcous.ddbgscan.core.CoreConfig.{UNKNOWN_CLUSTER}
import com.github.dmarcous.ddbgscan.model.ClusteringInstance
import org.apache.spark.sql.{Dataset, KeyValueGroupedDataset, SparkSession}

import scala.collection.JavaConverters._

object DataGeoClusterer {

  def clusterGeoData(@transient spark: SparkSession,
                     data: KeyValueGroupedDataset[Long, ClusteringInstance],
                     parameters: AlgorithmParameters
                    ): Dataset[(Long, List[ClusteringInstance])]  =
  {
    import spark.implicits._

    val clusteredData =
      data.mapGroups{case(key, vals) => (key,DataGeoClusterer.clusterLocalGeoData(key, vals, parameters))}

    clusteredData
  }

  def clusterLocalGeoData(
                     geoKey: Long,
                     data: Iterator[ClusteringInstance],
                     parameters: AlgorithmParameters
         ): List[ClusteringInstance] =
  {
    var currentCluster = UNKNOWN_CLUSTER

    val instances = data.toList
    val searchTree = PointSearchUtilities.buildPointGeometrySearchTree(instances)
    searchTree.entries().toBlocking().toIterable.asScala.foreach{
      // point - same p as in DBSCAN original implementation
      case (p) => {
        // Go over all unvisited points
        if (!p.value().isVisited)
        {
          // TODO : verify that this actually works and is mutable
          p.value().isVisited = true

          // Find neighbours
          val neighbours = NeighbourUtilities.getNeighbours(p,
            parameters.epsilon, searchTree, parameters.isNeighbourInstances)

          if (neighbours.size < parameters.minPts)
          {
            p.value().instanceStatus = NOISE.value
          }
          else
          {
            currentCluster += 1
            expandCluster(p, neighbours, searchTree, currentCluster)
          }

        }// traverse unvisited points
      }
    }// traverse points

    PointSearchUtilities.getClusteringInstancesFromSearchTree(searchTree)
  }

  def expandCluster(p: Entry[ClusteringInstance, Point], neighbours: List[Entry[ClusteringInstance, Point]],
                    searchTree: RTree[ClusteringInstance, Point], currentCluster: Long): Unit =
  {

  }



}
