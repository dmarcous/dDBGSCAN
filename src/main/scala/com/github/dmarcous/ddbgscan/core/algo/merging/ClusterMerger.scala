package com.github.dmarcous.ddbgscan.core.algo.merging

import com.github.dmarcous.ddbgscan.model.{ClusteringInstance, KeyGeoEntity}
import org.apache.spark.sql.{Dataset, SparkSession}

object ClusterMerger {

  def merge(@transient spark: SparkSession,
            clusteredData: Dataset[(Long, List[ClusteringInstance])]
         ): Dataset[(Long, List[ClusteringInstance])] =
  {
    // TODO : implement
    clusteredData
  }
}
