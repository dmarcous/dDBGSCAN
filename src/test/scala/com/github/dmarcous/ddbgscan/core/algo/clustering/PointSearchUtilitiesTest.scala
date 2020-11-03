package com.github.dmarcous.ddbgscan.core.algo.clustering

import com.github.davidmoten.rtree.geometry.Geometries
import com.github.dmarcous.ddbgscan.core.algo.partitioning.DataPartitionerS2
import com.github.dmarcous.ddbgscan.core.config.CoreConfig.{DEFAULT_GEO_FILE_DELIMITER, DEFAULT_LATITUDE_POSITION_FIELD_NUMBER, DEFAULT_LONGITUDE_POSITION_FIELD_NUMBER, MISSING_GEO_DECIMAL_SENSITIVITY_LVL, NO_UNIQUE_ID_FIELD}
import com.github.dmarcous.ddbgscan.core.config.IOConfig
import com.github.dmarcous.ddbgscan.core.preprocessing.GeoPropertiesExtractor
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import scala.collection.JavaConverters._

class PointSearchUtilitiesTest extends FlatSpec{

  val spark =
    SparkSession
      .builder()
      .master("local")
      .appName("PointSearchUtilitiesTest")
      .getOrCreate()
  import spark.implicits._

  val S2_LVL = 15
  val GEO_SENSITIVITY = MISSING_GEO_DECIMAL_SENSITIVITY_LVL
  val epsilon= 100.0

  val defaultLonLatDelimitedGeoData =
    spark.createDataset(
      Seq(
        // Pair cluster
        "34.778023,32.073889,6,7", //Address - Tsemach Garden
        "34.778137,32.074229,10,11", //Address - Dizzy21-25
        // NOISE
        "34.775628,32.074032,5,5", //Address - Voodoo
        //Triplet cluster
        "34.777112,32.0718015,8,9", //Address - Haimvelish1-7
        "34.777547,32.072729,1,2", //Address - BenTsiyon22-28
        "34.777558,32.072565,3,4" //Address - Warburg9-3
  ))

  val inputPath = "s3://my.bucket/ddbgscan/inputs/input.csv"
  val outputFolderPath = "s3://my.bucket/ddbgscan/outputs/"
  val positionId = NO_UNIQUE_ID_FIELD
  val positionLon = DEFAULT_LONGITUDE_POSITION_FIELD_NUMBER
  val positionLat = DEFAULT_LATITUDE_POSITION_FIELD_NUMBER
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

  val clusteringDataset =
    GeoPropertiesExtractor.fromLonLatDelimitedFile(
      spark, defaultLonLatDelimitedGeoData, GEO_SENSITIVITY, S2_LVL, ioConfig
    )
  val partitionedData = DataPartitionerS2.partitionData(spark, clusteringDataset, epsilon, S2_LVL)
  val instances = partitionedData.filter(_._1==1521455263322734592L).flatMap(_._2.toList).collect().toList.distinct

  "buildPointGeometrySearchTree" should "build a search tree from clustering instances" in
  {
    val searchTree = PointSearchUtilities.buildPointGeometrySearchTree(instances)

    println(instances.length)
    println(searchTree.asString())
    searchTree.size() should equal(5)
    val nearest = searchTree.nearest(Geometries.pointGeographic(34.777113,32.0718014),30,3)
    val nearList = nearest.toBlocking.toIterable.asScala.map{case(entry) => entry.value().lonLatLocation}.toList
    nearList should contain theSameElementsAs List(
      (34.777112,32.0718015), (34.777547,32.072729), (34.777558,32.072565)
    )
  }

  "getClusteringInstancesFromSearchTree" should "get all instances indexed in the search tree" in
  {
    val searchTree = PointSearchUtilities.buildPointGeometrySearchTree(instances)
    val treeInstances = PointSearchUtilities.getClusteringInstancesFromSearchTree(searchTree)
    treeInstances.map(_.lonLatLocation) should contain theSameElementsAs List(
      (34.777112,32.0718015), (34.777547,32.072729), (34.777558,32.072565),
      (34.778023,32.073889), (34.775628,32.074032)
    )
  }

  "getDensityReachablePointsFromSearchTree" should "get empty list if no points in range" in
  {
    val searchTree = PointSearchUtilities.buildPointGeometrySearchTree(instances)
    val reachableInstances = PointSearchUtilities.getGeoDensityReachablePointsFromSearchTree(
      searchTree, Geometries.pointGeographic(35.775629,32.074032), 100)

    reachableInstances.foreach(println)
    reachableInstances.length should equal(0)
  }
  it should "get a single point if in range" in
  {
    val searchTree = PointSearchUtilities.buildPointGeometrySearchTree(instances)
    val reachableInstances = PointSearchUtilities.getGeoDensityReachablePointsFromSearchTree(
      searchTree, Geometries.pointGeographic(34.775629,32.074032), 10)

    reachableInstances.foreach(println)
    reachableInstances.length should equal(1)
    reachableInstances.head.geometry().x() should equal (34.775628)
    reachableInstances.head.geometry().y() should equal (32.074032)
  }
  it should "get multiple reachable points in range if exist" in
  {
    val searchTree = PointSearchUtilities.buildPointGeometrySearchTree(instances)
    val reachableInstances = PointSearchUtilities.getGeoDensityReachablePointsFromSearchTree(
      searchTree, Geometries.pointGeographic(34.777548,32.072729), 40)

    reachableInstances.foreach(println)
    reachableInstances.length should equal(2)
    reachableInstances.map(instance => (instance.geometry().x(), instance.geometry().y())) should contain theSameElementsAs List(
      (34.777547,32.072729), (34.777558,32.072565)
    )
  }

}
