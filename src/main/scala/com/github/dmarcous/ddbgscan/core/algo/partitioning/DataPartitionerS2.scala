package com.github.dmarcous.ddbgscan.core.algo.partitioning

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
         ): Dataset[(Long, List[ClusteringInstance])] =
  {
    import spark.implicits._

    val cellReachableData =
      data
        // Using data from instance instead of key for safe conversion
        // (key isn't redundant as its used for inner /outer partitioning input validation)
        .map{case(key, instance) =>
          (key.s2CellId,
           DataPartitionerS2.getDensityReachableCells(
           instance.lonLatLocation._1, instance.lonLatLocation._2, neighborhoodPartitioningLvl, epsilon),
           instance)}

    // Key by expanded cell
    val keyValData =
      cellReachableData
        .flatMap{case(originalCellId, cells, instance) =>
          cells.map{case(expandedCellId) => (expandedCellId,
            instance.copy(isInExpandedGeom = expandedCellId != originalCellId)
          )}
        }

    // Remove cells that don't have any inner points (no original data)
    val reducedSizeData =
      keyValData
        .groupByKey(_._1) // group by expanded cell
        .mapValues(_._2) // remove redundant inner id
        // Sum number of inner instances
        .mapGroups{case(cellId,instances)=>
          (cellId,instances.toList)}
        .filter(!_._2.map(_.isInExpandedGeom).foldLeft(true)(_ && _)) // Keep only cells with at least 1 inner instance

    val repartitionedData =
      reducedSizeData
        .repartitionByRange(partitionExprs=$"_1") // Partition by cell id

    // Trigger shuffle for repartitioning to happen
    repartitionedData.count()

    repartitionedData
  }

  def getDensityReachableCells(pointGeoKey: KeyGeoEntity, epsilon: Double): List[Long] =
  {
    getDensityReachableCells(
      pointGeoKey.geoData(0).toDouble,
      pointGeoKey.geoData(1).toDouble,
      pointGeoKey.level,
      epsilon)
  }

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
