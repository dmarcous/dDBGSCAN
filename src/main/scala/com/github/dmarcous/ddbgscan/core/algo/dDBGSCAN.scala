package com.github.dmarcous.ddbgscan.core.algo

import com.github.dmarcous.ddbgscan.core.algo.clustering.DataGeoClusterer
import com.github.dmarcous.ddbgscan.core.algo.merging.ClusterMerger
import com.github.dmarcous.ddbgscan.core.algo.partitioning.DataPartitionerS2
import com.github.dmarcous.ddbgscan.core.config.AlgorithmParameters
import com.github.dmarcous.ddbgscan.model.{ClusteredInstance, ClusteringInstance, KeyGeoEntity}
import org.apache.spark.sql.{Dataset, SparkSession}

object dDBGSCAN {

  def run(@transient spark: SparkSession,
          data: Dataset[(KeyGeoEntity, ClusteringInstance)],
          parameters: AlgorithmParameters
         ): Dataset[ClusteredInstance] =
  {
    // Partition data w/ duplicates (density reachable)
    dDBGSCAN.setJobStageNameInSparkUI(spark, "Partitioning",
      "Stage 1 - Geo partitioning based on S2 Cells")
    val partitionedData =
      DataPartitionerS2.partitionData(spark, data,
        parameters.epsilon, parameters.neighborhoodPartitioningLvl)

    // Perform local clustering
    dDBGSCAN.setJobStageNameInSparkUI(spark, "Clustering",
      "Stage 2 - Cluster data locally in each partition using modified geo DBSCAN")
    val locallyClusteredData = DataGeoClusterer.clusterGeoData(spark, partitionedData, parameters)

    // Merge overlapping clusters to create a globally unique cluster map
    dDBGSCAN.setJobStageNameInSparkUI(spark, "Merging",
      "Stage 3 - Merge overlapping clusters from different partitions")
    val globallyClusteredData = ClusterMerger.merge(spark, locallyClusteredData)

    globallyClusteredData
  }

  def setJobStageNameInSparkUI(@transient spark: SparkSession, stageName: String, stageDescription: String): Unit =
  {
    spark.sparkContext.setJobGroup(stageName, stageDescription)
  }
}
