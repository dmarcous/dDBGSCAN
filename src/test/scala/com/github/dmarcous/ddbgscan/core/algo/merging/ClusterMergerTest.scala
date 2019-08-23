package com.github.dmarcous.ddbgscan.core.algo.merging

import com.github.dmarcous.ddbgscan.core.algo.clustering.DataGeoClusterer
import com.github.dmarcous.ddbgscan.core.algo.partitioning.DataPartitionerS2
import com.github.dmarcous.ddbgscan.core.config.CoreConfig.ClusteringInstanceStatusValue._
import com.github.dmarcous.ddbgscan.core.config.CoreConfig._
import com.github.dmarcous.ddbgscan.core.config.{AlgorithmParameters, IOConfig}
import com.github.dmarcous.ddbgscan.core.preprocessing.GeoPropertiesExtractor
import com.github.dmarcous.ddbgscan.model.ClusteringInstance
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class ClusterMergerTest extends FlatSpec{

  val spark =
    SparkSession
      .builder()
      .master("local")
      .appName("ClusterMergerTest")
      .getOrCreate()
  import spark.implicits._

  spark.sparkContext.setCheckpointDir("/tmp")

  val S2_LVL = 15
  val epsilon= 100.0

  val complexLonLatDelimitedGeoData =
    spark.createDataset(
      Seq(
        // Pair cluster
        "11,34.778023,32.073889,6,7", //Address - Tsemach Garden
        "12,34.778137,32.074229,10,11", //Address - Dizzy21-25
        "7,34.778982,32.074499,13,14", //Address - Shmaryahu Levin St 3
        "8,34.778998,32.075054,15,16", //Address - Shmaryahu Levin St 9,
        "9,34.779212,32.075217,17,18", //Address - Shmaryahu Levin St 14,
        "10,34.779861,32.075049,19,20", //Address - Sderot Chen 11,
        "1,34.779046,32.073903,21,22", //Adress - Tarsat
        "2,34.779765,32.073458,23,24", //Adress - HabimaSquare
        // NOISE
        "3,34.775628,32.074032,5,5", //Address - Voodoo
        //Triplet cluster
        "4,34.777112,32.0718015,8,9", //Address - Haimvelish1-7
        "5,34.777547,32.072729,11,2", //Address - BenTsiyon22-28
        "6,34.777558,32.072565,3,4" //Address - Warburg9-3
      ))

  val inputPath = "s3://my.bucket/ddbgscan/inputs/input.csv"
  val outputFolderPath = "s3://my.bucket/ddbgscan/outputs/"
  val positionId = 0
  val positionLon = DEFAULT_LONGITUDE_POSITION_FIELD_NUMBER+1
  val positionLat = DEFAULT_LATITUDE_POSITION_FIELD_NUMBER+1
  val delimiter = DEFAULT_GEO_FILE_DELIMITER
  val numPartitions = 1
  val ioConfig = IOConfig(
    inputPath,
    outputFolderPath,
    positionId,
    positionLon,
    positionLat,
    delimiter,
    numPartitions
  )

  val minPts = 3
  val params =
    AlgorithmParameters(
      epsilon,
      minPts,
      S2_LVL,
      DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION
    )

  val VOODO_CELL_KEY = 1521455263322734592L

  val clusteringDataset =
    GeoPropertiesExtractor.fromLonLatDelimitedFile(
      spark, complexLonLatDelimitedGeoData, S2_LVL, ioConfig
    )
  val partitionedData = DataPartitionerS2.partitionData(spark, clusteringDataset, epsilon, S2_LVL)
  val ungroupedPartitionedData = partitionedData.flatMap{case(key, vals) => (vals.map(instance => (key,instance)))}.collect()
  val locallyClusteredData = DataGeoClusterer.clusterGeoData(spark, partitionedData, params)
  val instanceStateData = ClusterMerger.computeInstanceState(spark, locallyClusteredData)
  val mergeClusterEdgeList = ClusterMerger.extractMergeCandidates(spark, instanceStateData)
  val mergedClustersToOriginalClustersMapping = ClusterMerger.mergeClusters(spark, mergeClusterEdgeList)

  "computeInstanceState" should "aggregate by point and compute aggregate instance status for a single partition" in
  {
    import spark.implicits._

    val voodooCellData = ungroupedPartitionedData.filter(_._1==VOODO_CELL_KEY)
    val clusteredData = DataGeoClusterer.clusterLocalGeoData(
      VOODO_CELL_KEY,
      voodooCellData.map(_._2).toList,
      params)
    val singleCellClusteredData: Dataset[(Long, List[ClusteringInstance])] =
      spark.createDataset(
        Seq((VOODO_CELL_KEY, clusteredData)
      ))
//    Input (singleCellClusteredData) =
//    ClusteringInstance(4,6,true,true,2,(34.777112,32.0718015),[8.0,9.0])
//    ClusteringInstance(6,6,true,true,1,(34.777558,32.072565),[3.0,4.0])
//    ClusteringInstance(5,6,true,true,2,(34.777547,32.072729),[11.0,2.0])
//    ClusteringInstance(1,-1,true,true,3,(34.778023,32.073889),[6.0,7.0])
//    ClusteringInstance(3,-1,true,false,3,(34.775628,32.074032),[5.0,5.0])
    val instanceStateData = ClusterMerger.computeInstanceState(spark, singleCellClusteredData)

    println("----State data----")
    //(recordId, preMergeCluster, preMergeInstanceStatus, isMergeCandidate, clustersContainingInstance)
    instanceStateData.collect.foreach(println)

    val stateData = instanceStateData.collect
    stateData.size should equal(5)
    val coreInstances = stateData.filter(_._3==CORE.value)
    coreInstances.size should equal(1)
    coreInstances.map(_._1) should contain theSameElementsAs (List(6))
    coreInstances.map(_._2).distinct should contain theSameElementsAs (List(6))
    val borderInstances = stateData.filter(_._3==BORDER.value)
    borderInstances.size should equal(2)
    borderInstances.map(_._1) should contain theSameElementsAs (List(4, 5))
    borderInstances.map(_._2).distinct should contain theSameElementsAs (List(6))
    val noiseInstances = stateData.filter(_._3==NOISE.value)
    noiseInstances.size should equal(2)
    noiseInstances.map(_._1) should contain theSameElementsAs (List(11, 3))
    noiseInstances.map(_._2).distinct should contain theSameElementsAs (List(UNKNOWN_CLUSTER))
    noiseInstances.map(_._5).flatMap{case(x)=>x}.distinct should contain theSameElementsAs (List(UNKNOWN_CLUSTER))
  }
  it should "aggregate by point and compute aggregate instance status for multiple partitions" in
  {
    val instanceStateData = ClusterMerger.computeInstanceState(spark, locallyClusteredData)

    println("----State data----")
    //(recordId, preMergeCluster, preMergeInstanceStatus, isMergeCandidate, clustersContainingInstance)
    val stateData = instanceStateData.collect
    stateData .foreach(println)

    stateData.size should equal(12)
    val coreInstances = stateData.filter(_._3==CORE.value)
    coreInstances.size should equal(8)
    coreInstances.map(_._2).distinct should contain theSameElementsAs (List(1,11,6))
    val borderInstances = stateData.filter(_._3==BORDER.value)
    borderInstances.size should equal(3)
    borderInstances.map(_._5).flatMap{case(x)=>x}.distinct should contain theSameElementsAs (List(1,11,6))
    val noiseInstances = stateData.filter(_._3==NOISE.value)
    noiseInstances.size should equal(1)
    noiseInstances.map(_._1) should contain theSameElementsAs (List(3))
    noiseInstances.map(_._2).distinct should contain theSameElementsAs (List(UNKNOWN_CLUSTER))
    noiseInstances.map(_._5).flatMap{case(x)=>x}.distinct should contain theSameElementsAs (List(UNKNOWN_CLUSTER))
  }

  "extractMergeCandidates" should "extract edge list of clusters to merge" in
  {
    val mergeClusterEdgeList = ClusterMerger.extractMergeCandidates(spark, instanceStateData)

    val mergeData = mergeClusterEdgeList.collect
    println("----Merge candidates----")
    // (src, dst)
    mergeData.foreach(println)

    mergeData.size should equal(1)
    List(mergeData.head.get(0), mergeData.head.get(1)) should contain theSameElementsAs (List(1, 11))
  }

  "mergeClusters" should "create a merged cluster mapping given 1 edge" in
  {
    val mergedClustersToOriginalClustersMapping =
      ClusterMerger.mergeClusters(spark, mergeClusterEdgeList)

    val mergedMapping = mergedClustersToOriginalClustersMapping.collect
    mergedMapping.foreach(println)
    mergedMapping.size should equal(2)
    mergedMapping.filter(_._1==1L).map(_._2) should contain theSameElementsAs (List(1))
    mergedMapping.filter(_._1==11L).map(_._2) should contain theSameElementsAs (List(1))
  }
  it should "create a merged cluster mapping given a complex graph with multiple connected components" in
  {
    import spark.implicits._

    val complexEdgeList =
      Seq(
        (1L, 3L),
        (2L, 4L),
        (8L, 3L),
        (4L, 1L),
        (6L, 7L),
        (5L, 6L),
        (9L, 10L)
      ).toDF("src", "dst")
    val mergedClustersToOriginalClustersMapping =
      ClusterMerger.mergeClusters(spark, complexEdgeList)

    // Returns (originalClusterId, mergedClusterId)
    val mergedMapping = mergedClustersToOriginalClustersMapping.collect
    mergedMapping.foreach(println)
    mergedMapping.size should equal(10)
    val distinctComponents = mergedMapping.map(_._2).distinct
    distinctComponents.size should equal(3)
    val clusterOf1 = mergedMapping.filter(_._1==1L).map(_._2).head
    val clusterOf5 = mergedMapping.filter(_._1==5L).map(_._2).head
    val clusterOf9 = mergedMapping.filter(_._1==9L).map(_._2).head
    mergedMapping.filter(_._2==clusterOf1).map(_._2).size should equal (5)
    mergedMapping.filter(_._2==clusterOf5).map(_._2).size should equal (3)
    mergedMapping.filter(_._2==clusterOf9).map(_._2).size should equal (2)
  }

  "createGlobalClusterMapping" should "join merged cluster mapping and original clusters to create a global mapping" in
  {
    val globallyClusteredData =
      ClusterMerger.createGlobalClusterMapping(spark, instanceStateData, mergedClustersToOriginalClustersMapping)

    // Returns ClusteredInstance
    val clusteringResults = globallyClusteredData.collect
    clusteringResults.foreach(println)
    clusteringResults.size should equal(12)
    clusteringResults.map(_.cluster).distinct.size should equal (3)
    clusteringResults.filter(_.instanceStatus == CORE.value).size should equal (8)
    clusteringResults.filter(_.instanceStatus == BORDER.value).size should equal (3)
    clusteringResults.filter(_.instanceStatus == NOISE.value).size should equal (1)
  }

  "merge" should "merge clusters" in
  {
    val globallyClusteredData =
      ClusterMerger.merge(spark, locallyClusteredData)

    // Returns ClusteredInstance
    val clusteringResults = globallyClusteredData.collect
    clusteringResults.foreach(println)
    clusteringResults.size should equal(12)
    clusteringResults.map(_.cluster).distinct.size should equal (3)
    clusteringResults.filter(_.instanceStatus == CORE.value).size should equal (8)
    clusteringResults.filter(_.instanceStatus == BORDER.value).size should equal (3)
    clusteringResults.filter(_.instanceStatus == NOISE.value).size should equal (1)
  }
}
