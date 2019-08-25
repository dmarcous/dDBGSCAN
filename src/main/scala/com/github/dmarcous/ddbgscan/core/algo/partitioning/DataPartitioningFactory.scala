package com.github.dmarcous.ddbgscan.core.algo.partitioning

import com.github.dmarcous.ddbgscan.core.config.AlgorithmParameters
import com.github.dmarcous.ddbgscan.model.{ClusteringInstance, KeyGeoEntity}
import org.apache.spark.sql.{Dataset, SparkSession}
import com.github.dmarcous.ddbgscan.core.config.CoreConfig.GEO_PARTITIONING_STRATEGY
import com.github.dmarcous.ddbgscan.core.config.CoreConfig.COST_PARTITIONING_STRATEGY

object DataPartitioningFactory {

  def partition(@transient spark: SparkSession,
                    data: Dataset[(KeyGeoEntity, ClusteringInstance)],
                    parameters: AlgorithmParameters
         ): Dataset[(Long, List[ClusteringInstance])] =
  {
    parameters.partitioningStrategy match {
      case GEO_PARTITIONING_STRATEGY => DataPartitionerS2.partitionData(
        spark, data, parameters.epsilon, parameters.neighborhoodPartitioningLvl)
      case COST_PARTITIONING_STRATEGY => DataPartitionerCostBased.partitionData(
        spark, data, parameters.epsilon, parameters.maxPointsPerPartition)
      case _ => throw new IllegalArgumentException
    }
  }
}
