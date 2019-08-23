package com.github.dmarcous.ddbgscan.core.algo.clustering

import com.github.dmarcous.ddbgscan.core.algo.clustering.NeighbourUtilities.applyUserDefinedFiltering
import com.github.dmarcous.ddbgscan.core.algo.partitioning.DataPartitionerS2
import com.github.dmarcous.ddbgscan.core.config.CoreConfig._
import com.github.dmarcous.ddbgscan.core.config.IOConfig
import com.github.dmarcous.ddbgscan.core.preprocessing.GeoPropertiesExtractor
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import scala.collection.JavaConverters._

class NeighbourUtilitiesTest extends FlatSpec{

  val spark =
    SparkSession
      .builder()
      .master("local")
      .appName("PointSearchUtilitiesTest")
      .getOrCreate()
  import spark.implicits._

  val S2_LVL = 15
  val epsilon= 130.0

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
        "34.777547,32.072729,11,2", //Address - BenTsiyon22-28
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
      spark, defaultLonLatDelimitedGeoData, S2_LVL, ioConfig
    )
  val partitionedData = DataPartitionerS2.partitionData(spark, clusteringDataset, epsilon, S2_LVL)
  val instances = partitionedData.filter(_._1==1521455263322734592L).flatMap(_._2.toList).collect().toList.distinct
  val searchTree = PointSearchUtilities.buildPointGeometrySearchTree(instances)

  val default_similarity_function = DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION
  val user_similarity_function = (x: Vector, y: Vector) => (x(0) < y(0))

  val FULL_NEIGHBOURHOOD_SIZE = 6
  val USER_REACHABLE_ELEMENTS_FEATURE_VALUES = List(6,8,10,11)
  val GEO_REACHABLE_ELEMENTS_FEATURE_VALUES = List(11,3)
  val NEIGHBOUR_REACHABLE_ELEMENTS_FEATURE_VALUES = List(11)

  "applyUserDefinedFiltering" should "return all neighbours if given default function" in
  {
    // setup
    val p = searchTree.entries().toBlocking.toIterable.asScala.toList.filter(_.geometry().x()==34.775628)
    p.size should equal(1)
    val initialNeighbourList = searchTree.entries().toBlocking.toIterable.asScala.toList
    initialNeighbourList.size should equal (FULL_NEIGHBOURHOOD_SIZE)

    val neighbours = applyUserDefinedFiltering(p.head, initialNeighbourList, default_similarity_function )
    neighbours.size should equal (FULL_NEIGHBOURHOOD_SIZE)

  }
  it should "return neighbours that pass user defined similarity function if exist" in
  {
    // setup
    //feature val = 5
    val p = searchTree.entries().toBlocking.toIterable.asScala.toList.filter(_.geometry().x()==34.775628)
    p.size should equal(1)
    val initialNeighbourList = searchTree.entries().toBlocking.toIterable.asScala.toList
    initialNeighbourList.size should equal (FULL_NEIGHBOURHOOD_SIZE)

    val neighbours = applyUserDefinedFiltering(p.head, initialNeighbourList, user_similarity_function )
    // should return feature vals > 5 -- 8,6,10,11
    neighbours.foreach(println)
    neighbours.size should equal (USER_REACHABLE_ELEMENTS_FEATURE_VALUES.size)
    neighbours.map(_.value().features(0)) should contain theSameElementsAs (USER_REACHABLE_ELEMENTS_FEATURE_VALUES)
  }

  "getNeighbours" should "return neighbours that pass user defined similarity function if exist" in
  {
    val p = searchTree.entries().toBlocking.toIterable.asScala.toList.filter(_.geometry().x()==34.777112)
    p.size should equal(1)
    val neighbours = NeighbourUtilities.getNeighbours(p.head, epsilon, searchTree, default_similarity_function)
    neighbours.foreach(println)
    neighbours.size should equal (GEO_REACHABLE_ELEMENTS_FEATURE_VALUES.size)
    neighbours.map(_.value().features(0)) should contain theSameElementsAs (GEO_REACHABLE_ELEMENTS_FEATURE_VALUES)
  }
  it should "filter neighbours that are geo reachable but don't pass user defined similarity function" in
  {
    val p = searchTree.entries().toBlocking.toIterable.asScala.toList.filter(_.geometry().x()==34.777112)
    p.size should equal(1)
    val neighbours = NeighbourUtilities.getNeighbours(p.head, epsilon, searchTree, user_similarity_function)
    neighbours.foreach(println)
    neighbours.size should equal (NEIGHBOUR_REACHABLE_ELEMENTS_FEATURE_VALUES.size)
    neighbours.map(_.value().features(0)) should contain theSameElementsAs (NEIGHBOUR_REACHABLE_ELEMENTS_FEATURE_VALUES)
  }


}
