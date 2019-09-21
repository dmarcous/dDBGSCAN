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
      GEO_PARTITIONING_STRATEGY,
      S2_LVL,
      DEFAULT_NUM_PARTITIONS,
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

//  // Edge case test
//  "Edge case test" should " return same clustering with 2 different s2 lvls " in
//    {
//      val ds =
//        spark.createDataset(
//          Seq(
//            "1,1.539409,42.511316",
//            "65,1.5368600,42.51123",
//            "100,1.536462,42.512279",
//            "101,1.537420,42.510676",
//            "102,1.537816,42.510363",
//            "103,1.538486,42.510051",
//            "104,1.539297,42.509750",
//            "105,1.540198,42.509355",
//            "106,1.543234,42.509157",
//            "107,1.543970,42.509011",
//            "108,1.545131,42.509192",
//            "109,1.546056,42.509769",
//            "110,1.547028,42.510242",
//            "111,1.547834,42.510673",
//            "796,1.55108,42.51325",
//            "1000,1.549808,42.512417",
//            "2542,1.54782,42.51209",
//            "3054,1.54829,42.5116"
//      ))
//      val positionId = 0
//      val positionLon = DEFAULT_LONGITUDE_POSITION_FIELD_NUMBER+1
//      val positionLat = DEFAULT_LATITUDE_POSITION_FIELD_NUMBER+1
//      val delimiter = DEFAULT_GEO_FILE_DELIMITER
//      val numPartitions = 1
//      val ioConfig = IOConfig(
//        inputPath,
//        outputFolderPath,
//        positionId,
//        positionLon,
//        positionLat,
//        delimiter,
//        numPartitions
//      )
//      spark.sparkContext.setCheckpointDir("/tmp")
//
//      val minPts = 8 //5
//      val epsilon = 300.0 //100.0
//      val lvl2 = 14
//      val params2 =
//        AlgorithmParameters(
//          epsilon,
//          minPts,
//          GEO_PARTITIONING_STRATEGY,
//          lvl2,
//          DEFAULT_NUM_PARTITIONS,
//          DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION
//        )
//      println("epsilon " + params2.epsilon)
//      println("lvl " + params2.neighborhoodPartitioningLvl)
//      val lvl2_clusteringDataset =
//        GeoPropertiesExtractor.fromLonLatDelimitedFile(
//          spark, ds, lvl2, ioConfig
//        )
//      val lvl2_partitionedData = DataPartitionerS2.partitionData(spark, lvl2_clusteringDataset, epsilon, lvl2)
//      val lvl2_locallyClusteredData = DataGeoClusterer.clusterGeoData(spark, lvl2_partitionedData, params2)
//      val lvl2_instanceStateData = ClusterMerger.computeInstanceState(spark, lvl2_locallyClusteredData)
//      val lvl2_mergeClusterEdgeList = ClusterMerger.extractMergeCandidates(spark, lvl2_instanceStateData)
//      val lvl2_mergedClustersToOriginalClustersMapping = ClusterMerger.mergeClusters(spark, lvl2_mergeClusterEdgeList)
//      val lvl2_globallyClusteredData =
//        createGlobalClusterMapping(spark, lvl2_instanceStateData, lvl2_mergedClustersToOriginalClustersMapping)
//
//      println("lvl2_mergedClustersToOriginalClustersMapping")
//      lvl2_mergedClustersToOriginalClustersMapping.collect.foreach(println)
//      println("lvl2_globallyClusteredData")
//      lvl2_globallyClusteredData.collect.foreach(println)
//
//    }
}
