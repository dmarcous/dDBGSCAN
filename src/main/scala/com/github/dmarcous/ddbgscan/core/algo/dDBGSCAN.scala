package com.github.dmarcous.ddbgscan.core.algo

import com.github.dmarcous.ddbgscan.core.algo.clustering.DataGeoClusterer
import com.github.dmarcous.ddbgscan.core.algo.merging.ClusterMerger
import com.github.dmarcous.ddbgscan.core.algo.partitioning.DataPartitionerS2
import com.github.dmarcous.ddbgscan.core.config.AlgorithmParameters
import com.github.dmarcous.ddbgscan.model.{ClusteringInstance, KeyGeoEntity}
import org.apache.spark.sql.{Dataset, SparkSession}

object dDBGSCAN {

  def run(@transient spark: SparkSession,
          data: Dataset[(KeyGeoEntity, ClusteringInstance)],
          parameters: AlgorithmParameters
         ): Dataset[String] =
  {
    import spark.implicits._

    // Partition data w/ duplicates (density reachable)
    val partitionedData =
      DataPartitionerS2.partitionData(spark, data,
        parameters.epsilon, parameters.neighborhoodPartitioningLvl)

    // Perform local clustering
    val locallyClusteredData = DataGeoClusterer.clusterGeoData(spark, partitionedData, parameters)

    // Perform global merging
    // TODO : maybe need to comeback to recordId to avoid duplicates in merge
    val globallyClusteredData = ClusterMerger.merge(spark, locallyClusteredData)

    // TODO : change return type to clusteringInstance, put key in RecordID - move to string in main
    globallyClusteredData.map(_._2.toString())
  }
}
