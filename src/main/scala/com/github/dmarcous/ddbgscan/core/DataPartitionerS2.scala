package com.github.dmarcous.ddbgscan.core

import java.util.ArrayList
import com.github.dmarcous.ddbgscan.model.{ClusteringInstance, KeyGeoEntity}
import com.github.dmarcous.s2utils.converters.UnitConverters
import com.google.common.geometry.{S2Cap, S2CellId, S2LatLng, S2RegionCoverer}
import org.apache.spark.sql.{Dataset, KeyValueGroupedDataset, SparkSession}
import scala.collection.JavaConverters._

object DataPartitionerS2 {

  def partitionData(@transient spark: SparkSession,
                    data: Dataset[(KeyGeoEntity, ClusteringInstance)],
                    epsilon: Double,
                    neighborhoodPartitioningLvl : Int
         ): KeyValueGroupedDataset[Long, ClusteringInstance] =
  {
    import spark.implicits._

    val cellReachableData = data.map{case(key, instance) =>
      (DataPartitionerS2.getDensityReachableCells(
        instance.lonLatLocation._1, instance.lonLatLocation._2, neighborhoodPartitioningLvl, epsilon), instance)}
    val keyValData = cellReachableData.flatMap{case(cells, instance) => cells.map{case(cellId) => (cellId, instance)}}
    val groupedData = keyValData.groupByKey(_._1).mapValues(_._2)

    groupedData
  }

  //TODO : test
  def getDensityReachableCells(lon: Double, lat: Double, level : Int, epsilon: Double): List[Long] =
  {
    val startPoint = S2LatLng.fromDegrees(lat, lon).normalized().toPoint
    val regionToCover = S2Cap.fromAxisHeight(
      startPoint,
      UnitConverters.metricToUnitSphereArea(epsilon*epsilon)/2)

    var coveringCells = new ArrayList[S2CellId]()
    S2RegionCoverer.getSimpleCovering(regionToCover, startPoint, level, coveringCells)

    val densityReachableCells = coveringCells.asScala.map(_.id).toList

    densityReachableCells
  }
}
