package com.github.dmarcous.ddbgscan.core

import com.github.davidmoten.rtree.{Entries, RTree}
import com.github.davidmoten.rtree.geometry.{Geometries, Point}
import com.github.dmarcous.ddbgscan.core.CoreConfig.UNKNOWN_CLUSTER
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
    val entryList = data.map(instance =>
      Entries.entry(instance,
                    Geometries.pointGeographic(
                      instance.lonLatLocation._1,
                      instance.lonLatLocation._2))).toList.asJava
    val searchTree = RTree.star().create[ClusteringInstance, Point](entryList)

    var curCluster = UNKNOWN_CLUSTER

    searchTree.entries().toBlocking().toIterable.asScala.foreach{
      case (x) =>
      x
    }

    searchTree.entries().toBlocking().toIterable.asScala.map{case(entry) => entry.value()}.toList
  }
}
