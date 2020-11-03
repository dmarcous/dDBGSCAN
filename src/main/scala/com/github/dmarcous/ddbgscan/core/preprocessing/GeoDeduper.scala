package com.github.dmarcous.ddbgscan.core.preprocessing

import com.github.dmarcous.ddbgscan.core.config.CoreConfig.{ClusteringInstanceStatusValue, DEFAULT_GEO_FILE_DELIMITER, DEFAULT_RECORD_ID, MISSING_GEO_DECIMAL_SENSITIVITY_LVL, NO_UNIQUE_ID_FIELD}
import com.github.dmarcous.ddbgscan.core.config.{AlgorithmParameters, IOConfig}
import com.github.dmarcous.ddbgscan.model.{ClusteredInstance, ClusteringInstance, KeyGeoEntity, LonLatGeoEntity}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.col

object GeoDeduper {

  def dedupByInstanceGeo(@transient spark: SparkSession,
                              data: Dataset[(KeyGeoEntity, ClusteringInstance)],
                              parameters: AlgorithmParameters)
  : Dataset[(KeyGeoEntity, ClusteringInstance)] =
  {
    import spark.implicits._

    // Dedup on
    if (parameters.geoDecimalPlacesSensitivity != MISSING_GEO_DECIMAL_SENSITIVITY_LVL)
      {
        data
          .map{case(key, instance) => (instance.lonLatLocation, (key, instance))}
          .toDF("instance_geo", "key_val")
          .dropDuplicates("instance_geo")
          .select("key_val._1", "key_val._2")
          .as[(KeyGeoEntity, ClusteringInstance)]
      }
    else
      {
        data
      }
  }

  def restoreDupesToClusters(@transient spark: SparkSession,
                             globallyClusteredData: Dataset[ClusteredInstance],
                             originalData: Dataset[(KeyGeoEntity, ClusteringInstance)])
  : Dataset[ClusteredInstance] =
    {
      import spark.implicits._

      val originalFormattedInstances =
        originalData
        .map{case(key, instance) => (instance.recordId, instance.lonLatLocation)}
        .toDF("record_id", "geo_dupe_key")
        .as("original")

      val ClusteredFormattedInstances =
        globallyClusteredData
        .map(instance => (instance.recordId, instance.cluster, instance.instanceStatus))
        .toDF("record_id", "cluster", "status")
        .as("clustered")

      val originalAndClusteredInstances =
        originalFormattedInstances
        .joinWith(
          ClusteredFormattedInstances,
          originalFormattedInstances("record_id")===ClusteredFormattedInstances("record_id"),
          "left_outer"
        )
        .selectExpr(
          "_1.record_id as record_id",
          "_1.geo_dupe_key as geo_dupe_key",
          "_2.cluster as cluster",
          "_2.status as status"
        ).as("joined")

      val dupeKeyClusters =
        originalAndClusteredInstances
        .where("cluster is not null")
        .groupBy("geo_dupe_key")
        .agg(max("cluster").as("cluster"),
             max("status").as("status"))
        .select("geo_dupe_key", "cluster", "status")
        .as("keys")

      val expandedClusteredSet =
        originalAndClusteredInstances.as("joined")
        .joinWith(
          dupeKeyClusters.as("keys"),
          col("joined.geo_dupe_key")===col("keys.geo_dupe_key"),
          "inner"
        )
        .as("expanded")
        .selectExpr(
          "_1.record_id as recordId",
          "_2.cluster as cluster",
          "_2.status as instanceStatus"
        ).as[ClusteredInstance]

      expandedClusteredSet
    }
}
