package com.github.dmarcous.ddbgscan.core.algo.clustering

import com.github.dmarcous.ddbgscan.core.algo.merging.ClusterMerger
import com.github.dmarcous.ddbgscan.core.algo.merging.ClusterMerger.createGlobalClusterMapping
import com.github.dmarcous.ddbgscan.core.algo.partitioning.DataPartitionerS2
import com.github.dmarcous.ddbgscan.core.config.CoreConfig.ClusteringInstanceStatusValue._
import com.github.dmarcous.ddbgscan.core.config.CoreConfig._
import com.github.dmarcous.ddbgscan.core.config.{AlgorithmParameters, IOConfig}
import com.github.dmarcous.ddbgscan.core.preprocessing.GeoPropertiesExtractor
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class DataGeoClustererTest extends FlatSpec{

  val spark =
    SparkSession
      .builder()
      .master("local")
      .appName("DataGeoClustererTest")
      .getOrCreate()
  import spark.implicits._

  val S2_LVL = 15
  val GEO_SENSITIVITY = MISSING_GEO_DECIMAL_SENSITIVITY_LVL
  val epsilon= 100.0

  val complexLonLatDelimitedGeoData =
    spark.createDataset(
      Seq(
        // Pair cluster
        "1,34.778023,32.073889,6,7", //Address - Tsemach Garden
        "2,34.778137,32.074229,10,11", //Address - Dizzy21-25
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
      MISSING_GEO_DECIMAL_SENSITIVITY_LVL,
      S2_LVL,
      DEFAULT_NUM_PARTITIONS,
      DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION
    )

  val VOODO_CELL_KEY = 1521455263322734592L

  val clusteringDataset =
    GeoPropertiesExtractor.fromLonLatDelimitedFile(
      spark, complexLonLatDelimitedGeoData, GEO_SENSITIVITY, S2_LVL, ioConfig
    )
  val partitionedData = DataPartitionerS2.partitionData(spark, clusteringDataset, epsilon, S2_LVL)
  val ungroupedPartitionedData = partitionedData.flatMap{case(key, vals) => (vals.map(instance => (key,instance)))}.collect()

  "clusterLocalGeoData" should " geo cluster given instances with regard to types (core, border, noise) " in
  {
    //    //voodo cell
    //    (1521455263322734592,ClusteringInstance(1,0,false,4,(34.778023,32.073889),[6.0,7.0])) //Noise (in another cell - C1) - Address - Tsemach Garden
    //    (1521455263322734592,ClusteringInstance(1,0,false,4,(34.775628,32.074032),[5.0,5.0])) // Noise - Address - Voodoo
    //    (1521455263322734592,ClusteringInstance(1,0,false,4,(34.777112,32.0718015),[8.0,9.0])) //C2-A - Address - Haimvelish1-7
    //    (1521455263322734592,ClusteringInstance(1,0,false,4,(34.777547,32.072729),[11.0,2.0]))//C2-B - Address - BenTsiyon22-28
    //    (1521455263322734592,ClusteringInstance(1,0,false,4,(34.777558,32.072565),[3.0,4.0]))//C2-C - Address - Warburg9-3
    // distances : C2-A , C2-C = 90
    // distances : C2-A , C2-B = 110
    // distances : C2-C , C2-B = 20

    val voodooCellData = ungroupedPartitionedData.filter(_._1==VOODO_CELL_KEY)
    val clusteredData = DataGeoClusterer.clusterLocalGeoData(
      VOODO_CELL_KEY,
      voodooCellData.map(_._2).toList,
      params)

    println("----Clustering results----")
    clusteredData.foreach(println)

    // Make sure all instances were visited
    val visitedInstances = clusteredData.filter(_.isVisited == true)
    visitedInstances.size should equal(clusteredData.size)
    // Make sure cluster was correctly detected
    val clusterDetectedInstances = clusteredData.filter(_.cluster != UNKNOWN_CLUSTER)
    val numberOfClustersIdentified = clusterDetectedInstances.map(_.cluster).distinct.size
    numberOfClustersIdentified should equal(1)
    clusterDetectedInstances.map(_.features(0)) should contain theSameElementsAs (List(3.0, 8.0, 11.0))
    val coreInstances = clusterDetectedInstances.filter(_.instanceStatus==CORE.value)
    coreInstances.size should equal(1)
    coreInstances.map(_.features(0)) should contain theSameElementsAs (List(3.0))
    val borderInstances = clusterDetectedInstances.filter(_.instanceStatus==BORDER.value)
    borderInstances.size should equal(2)
    borderInstances.map(_.features(0)) should contain theSameElementsAs (List(8.0, 11.0))
    // Make sure noise was correctly detected
    val noiseInstances = clusteredData.filter(_.instanceStatus == NOISE.value)
    noiseInstances.size should equal(2)
    noiseInstances.map(_.features(0)) should contain theSameElementsAs (List(5.0, 6.0))
  }

}
