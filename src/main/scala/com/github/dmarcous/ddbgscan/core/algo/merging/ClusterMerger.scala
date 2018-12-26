package com.github.dmarcous.ddbgscan.core.algo.merging

import com.github.dmarcous.ddbgscan.core.config.CoreConfig.ClusteringInstanceStatusValue.{CORE, NOISE, UNKNOWN}
import com.github.dmarcous.ddbgscan.core.config.CoreConfig.UNKNOWN_CLUSTER
import com.github.dmarcous.ddbgscan.model.{ClusteredInstance, ClusteringInstance}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}
import org.graphframes.GraphFrame

object ClusterMerger {

  def merge(@transient spark: SparkSession,
            clusteredData: Dataset[(Long, List[ClusteringInstance])]
         ): Dataset[ClusteredInstance] =
  {
    // Group by instance and compute state / merge status / participating clusters
    val instanceStateData: Dataset[(Long, Long, Int, Boolean, List[Long])] =
      computeInstanceState(spark, clusteredData)

    // Get merge candidates (edge list of connected clusters)
    val mergeClusterEdgeList: DataFrame =
      extractMergeCandidates(spark, instanceStateData)

    val mergedClustersToOriginalClustersMapping =
      mergeClusters(spark, mergeClusterEdgeList)

    val globallyClusteredData =
      createGlobalClusterMapping(spark, instanceStateData, mergedClustersToOriginalClustersMapping)

    globallyClusteredData
  }


  // Group by instance and compute state / merge status / participating clusters
  def computeInstanceState(@transient spark: SparkSession,
                           clusteredData: Dataset[(Long, List[ClusteringInstance])]): Dataset[(Long, Long, Int, Boolean, List[Long])] = {
    import spark.implicits._

    val locallyClusteredData =
      clusteredData
        .flatMap { case (cellId, instances) => (instances.map((instance) => (cellId, instance))) }
        .map { case (cellId, instance) => (instance.recordId, instance) }

    val instanceStateData =
      locallyClusteredData
        .groupByKey(_._1)
        .mapValues(_._2)
        .mapGroups { case (recordId, instances) => {
          val (preMergeInstanceStatus: Int,
          preMergeCluster: Long,
          existsAsCoreInner: Boolean,
          existsAsNotNoiseOuter: Boolean,
          clustersContainingInstance: List[Long]
            ) =
            instances.foldLeft(UNKNOWN.value, UNKNOWN_CLUSTER, false, false, List[Long]()) {
              case (agg, instance) => {
                (agg._1.min(instance.instanceStatus), // core < border < noise < unknown
                  if (instance.cluster != UNKNOWN_CLUSTER ||
                    instance.instanceStatus != NOISE.value)
                  {
                    // First cluster compares to -1 so we can't use min function
                    if (agg._2 == UNKNOWN_CLUSTER) instance.cluster else agg._2.min(instance.cluster)
                  }
                  else agg._2, // get clusterId from assigned records
                  agg._3 || (instance.instanceStatus == CORE.value && !instance.isInExpandedGeom),
                  agg._4 || (instance.instanceStatus != NOISE.value && instance.isInExpandedGeom),
                  agg._5.+:(instance.cluster)
                )
              }
            }

          // Remove noise cluster to not count it as an edge for merge if we have other clusters assigned
          val uniqueClustersContainingInstance =
            if (preMergeCluster != UNKNOWN_CLUSTER) clustersContainingInstance.filter(_!=UNKNOWN_CLUSTER).distinct
            else  clustersContainingInstance.distinct
          val isMergeCandidate = existsAsCoreInner && existsAsNotNoiseOuter &&
            (uniqueClustersContainingInstance.size > 1)

          (recordId, preMergeCluster, preMergeInstanceStatus,
            isMergeCandidate, uniqueClustersContainingInstance)
        }}

    instanceStateData
  }

  // Find merge candidates by filtering on state, prepare edge list on related clusters for graph merging
  def extractMergeCandidates(@transient spark: SparkSession,
                             instanceStateData: Dataset[(Long, Long, Int, Boolean, List[Long])]): DataFrame =
  {
    import spark.implicits._

    val mergeCandidateInstances =
      instanceStateData.filter(_._4)
        .map{case(recordId, preMergeCluster, preMergeInstanceStatus, isMergeCandidate, clustersContainingInstance) =>
        (clustersContainingInstance)}
        .distinct
    val mergeClusterEdgeList =
      mergeCandidateInstances
        .flatMap{case(clusters) => clusters.zip(clusters.tail)}
        .toDF("src", "dst")

    mergeClusterEdgeList
  }

  def mergeClusters(@transient spark: SparkSession,
                    mergeClusterEdgeList: DataFrame): Dataset[(Long, Long)] =
  {
    import spark.implicits._

    val clustersGraph =
    GraphFrame.fromEdges(mergeClusterEdgeList)
    // returns ("id", "component")
    val clusterToComponentMapping = clustersGraph.connectedComponents.run()

    val minimumClusterIdPerComponent =
      clusterToComponentMapping
        .groupBy("component")
        .agg(functions.min("id").as("minId"))

    val mergedClustersToOriginalClustersMapping =
      clusterToComponentMapping.as("corig")
        .join(minimumClusterIdPerComponent.as("cmin"),
              clusterToComponentMapping.col("component") ===
              minimumClusterIdPerComponent.col("component"),
              "inner")
        .selectExpr("id as originalClusterId",
                    "minId as mergedClusterId")
        .as[(Long, Long)]

    mergedClustersToOriginalClustersMapping
  }

  def createGlobalClusterMapping(@transient spark: SparkSession,
           instanceStateData: Dataset[(Long, Long, Int, Boolean, List[Long])],
           mergedClustersToOriginalClustersMapping: Dataset[(Long, Long)]): Dataset[ClusteredInstance] =
  {
    import spark.implicits._

    val preMergeClusteredData =
      instanceStateData
        .map{case(recordId, preMergeCluster, preMergeInstanceStatus, isMergeCandidate, clustersContainingInstance) =>
          (recordId, preMergeCluster, preMergeInstanceStatus)}

    val globalClusterMapping =
     preMergeClusteredData
       .joinWith(mergedClustersToOriginalClustersMapping,
                 preMergeClusteredData("_2")===mergedClustersToOriginalClustersMapping("originalClusterId"),
                 "left_outer")

    val globallyClusteredData =
      globalClusterMapping
          .map{case(instance, clusterMapping) =>
            ClusteredInstance(
              recordId=instance._1,
              // Take global merged cluster if exists
              cluster=(if(clusterMapping != null) clusterMapping._2 else instance._2),
              instanceStatus=instance._3
            )
          }

    globallyClusteredData
  }
}
