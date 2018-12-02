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
    val partitionedData =
      DataPartitionerS2.partitionData(spark, data,
        parameters.epsilon, parameters.neighborhoodPartitioningLvl)

    // Perform local clustering
    val locallyClusteredData = DataGeoClusterer.clusterGeoData(spark, partitionedData, parameters)

    // Merge overlapping clusters to create a globally unique cluster map
    val globallyClusteredData = ClusterMerger.merge(spark, locallyClusteredData)

    globallyClusteredData
  }
}
