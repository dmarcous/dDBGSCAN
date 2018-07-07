package com.github.dmarcous.ddbgscan.core

import com.github.dmarcous.ddbgscan.model.{ClusteringInstance, KeyGeoEntity}
import org.apache.spark.sql.{Dataset, KeyValueGroupedDataset, SparkSession}

object DataPartitionerS2 {

  def partitionData(@transient spark: SparkSession,
                    data: Dataset[(KeyGeoEntity, ClusteringInstance)]
         ): KeyValueGroupedDataset[Long, ClusteringInstance] =
  {
    import spark.implicits._

    // Partition data
    val keyValData = data.map{case(key, instance) => (key.s2CellId, instance)}

    val groupedData = keyValData.groupByKey(_._1).mapValues(_._2)

    groupedData
  }
}
