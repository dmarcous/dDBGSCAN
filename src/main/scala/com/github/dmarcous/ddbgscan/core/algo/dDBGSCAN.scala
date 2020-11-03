package com.github.dmarcous.ddbgscan.core.algo

import com.github.dmarcous.ddbgscan.core.algo.clustering.DataGeoClusterer
import com.github.dmarcous.ddbgscan.core.algo.merging.ClusterMerger
import com.github.dmarcous.ddbgscan.core.algo.partitioning.{DataPartitionerS2, DataPartitioningFactory}
import com.github.dmarcous.ddbgscan.core.config.AlgorithmParameters
import com.github.dmarcous.ddbgscan.core.config.CoreConfig.MISSING_GEO_DECIMAL_SENSITIVITY_LVL
import com.github.dmarcous.ddbgscan.core.preprocessing.GeoDeduper
import com.github.dmarcous.ddbgscan.model.{ClusteredInstance, ClusteringInstance, KeyGeoEntity}
import org.apache.spark.sql.{Dataset, SparkSession}

object dDBGSCAN {

  def run(@transient spark: SparkSession,
          data: Dataset[(KeyGeoEntity, ClusteringInstance)],
          parameters: AlgorithmParameters,
          debug: Boolean
         ): Dataset[ClusteredInstance] =
  {
    // Dedupe geo data based on sensitivity
    dDBGSCAN.setJobStageNameInSparkUI(spark, "Deduping",
      "Stage 0.5 - Geo Deduping")
    val dedupedData =
      GeoDeduper.dedupByInstanceGeo(spark, data, parameters)

    // Partition data w/ duplicates (density reachable)
    dDBGSCAN.setJobStageNameInSparkUI(spark, "Partitioning",
      "Stage 1 - Data partitioning")
    val partitionedData =
      DataPartitioningFactory.partition(spark, dedupedData, parameters)
    // Trigger ops before clustering for time measurements
    // [already triggered, last row in DataPartitionerS2.partitionData]

    // Perform local clustering
    dDBGSCAN.setJobStageNameInSparkUI(spark, "Clustering",
      "Stage 2 - Cluster data locally in each partition using modified geo DBSCAN")
    val locallyClusteredData = DataGeoClusterer.clusterGeoData(spark, partitionedData, parameters)

    // Trigger ops before merge for time measurements
    if(debug) locallyClusteredData.count()

    // Merge overlapping clusters to create a globally unique cluster map
    dDBGSCAN.setJobStageNameInSparkUI(spark, "Merging",
      "Stage 3 - Merge overlapping clusters from different partitions")
    val globallyClusteredData = ClusterMerger.merge(spark, locallyClusteredData)

    // Trigger ops before output for time measurements
    if(debug) globallyClusteredData.count()

    // Restore duplicate data and assign to clusters
    if(parameters.geoDecimalPlacesSensitivity != MISSING_GEO_DECIMAL_SENSITIVITY_LVL)
    {
      dDBGSCAN.setJobStageNameInSparkUI(spark, "Duping",
        "Stage 3.5 - Restoring deduped data to clusters")
      GeoDeduper.restoreDupesToClusters(spark, globallyClusteredData, data)
    }
    else globallyClusteredData
  }

  def setJobStageNameInSparkUI(@transient spark: SparkSession, stageName: String, stageDescription: String): Unit =
  {
    spark.sparkContext.setJobGroup(stageName, stageDescription)
  }
}
