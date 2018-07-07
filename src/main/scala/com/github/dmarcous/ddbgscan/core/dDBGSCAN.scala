package com.github.dmarcous.ddbgscan.core

import com.github.dmarcous.ddbgscan.model.{ClusteringInstance, KeyGeoEntity}
import org.apache.spark.sql.{Dataset, SparkSession}

object dDBGSCAN {

  def run(@transient spark: SparkSession,
          data: Dataset[(KeyGeoEntity, ClusteringInstance)],
          parameters: AlgorithmParameters
         ): Dataset[String] =
  {
    // Partition data
    val partitionedData = DataPartitionerS2.partitionData(spark, data)

    // Perform local clustering
    val locallyClusteredData = DataGeoClusterer.clusterGeoData(spark, partitionedData, parameters)

    // Perform global merging
    val globallyClusteredData = null

    globallyClusteredData
  }
}
