package com.github.dmarcous.ddbgscan.core

import com.github.dmarcous.ddbgscan.core.CoreConfig.{DEFAULT_GEO_FILE_DELIMITER, DEFAULT_LATITUDE_POSITION_FIELD_NUMBER, DEFAULT_LONGITUDE_POSITION_FIELD_NUMBER, NO_UNIQUE_ID_FIELD}
import com.github.dmarcous.ddbgscan.model.{KeyGeoEntity, LonLatGeoEntity}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class DataPartitionerS2Test extends FlatSpec{

  val spark =
    SparkSession
      .builder()
      .master("local")
      .appName("DataPartitionerS2Test")
      .getOrCreate()
  import spark.implicits._

  val S2_LVL = 15
  val epsilon_in_range = 10.0
  val epsilon_partly_outside_range = 100.0
  val epsilon_fully_outside_range = 250.0
  val GEO_DATA_KEYS = Array(1521455269765185536L, 1521455263322734592L,
    1521455261175250944L, 1521455259027767296L)
  val GEO_DATA_VALUES =
    Array(
      Array(1.0, 2.0, 3.0, 4.0, 5.0),
      Array(6.0, 7.0, 8.0, 9.0, 10.0),
      Array(1.0, 2.0, 3.0, 4.0, 5.0),
      Array(6.0, 7.0, 8.0, 9.0, 10.0),
      Array(1.0, 2.0, 3.0, 4.0, 5.0),
      Array(1.0, 2.0, 3.0, 4.0, 5.0)
    )

  val lon = 34.777547
  val lat = 32.072729

  val defaultLonLatDelimitedGeoData =
    spark.createDataset(
      Seq(
        "34.777547,32.072729,1,2,3,4,5",
        "34.776325,32.073905,6,7,8,9,10"
      ))
  val complexLonLatDelimitedGeoData =
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
  val ioConfig = IOConfig(
    inputPath,
    outputFolderPath,
    positionId,
    positionLon,
    positionLat,
    delimiter
  )

  val clusteringDataset =
    GeoPropertiesExtractor.fromLonLatDelimitedFile(
      spark, defaultLonLatDelimitedGeoData, S2_LVL, ioConfig
    )
  val complexDataset =
    GeoPropertiesExtractor.fromLonLatDelimitedFile(
      spark, complexLonLatDelimitedGeoData, S2_LVL, ioConfig
    )

  "getDensityReachableCells" should "get current cell if epsilon is small" in
  {
    val observedCells = DataPartitionerS2.getDensityReachableCells(lon, lat, S2_LVL, epsilon_in_range)
    val expectedCells = List(1521455269765185536L)

    observedCells should equal (expectedCells)

  }
  it should "get adjacent cells if epsilon crosses part of edges them" in
  {
    val observedCells = DataPartitionerS2.getDensityReachableCells(lon, lat, S2_LVL, epsilon_partly_outside_range)
    val expectedCells = List(
      1521455269765185536L, 1521455263322734592L,
      1521455261175250944L, 1521455259027767296L)

    observedCells should equal (expectedCells)

  }
  it should "get all adjacent cells if epsilon crosses all edges" in
  {
    val observedCells = DataPartitionerS2.getDensityReachableCells(lon, lat, S2_LVL, epsilon_fully_outside_range)
    val expectedCells = List(
      1521455269765185536L, 1521455263322734592L, 1521455265470218240L,
      1521455261175250944L, 1521455267617701888L, 1521455278355120128L,
      1521455271912669184L, 1521455231110479872L, 1521455259027767296L)

    observedCells should equal (expectedCells)

  }
  it should "get all adjacent cells if epsilon crosses all edges from point geo entity" in
  {
    val observedCells = DataPartitionerS2.getDensityReachableCells(
      new KeyGeoEntity(LonLatGeoEntity(lon, lat), S2_LVL),
      epsilon_fully_outside_range)
    val expectedCells = List(
      1521455269765185536L, 1521455263322734592L, 1521455265470218240L,
      1521455261175250944L, 1521455267617701888L, 1521455278355120128L,
      1521455271912669184L, 1521455231110479872L, 1521455259027767296L)

    observedCells should equal (expectedCells)
  }

  "partitionData" should "transform a geo delimited dataset with default properties to a geo clustering input dataset" in
  {
    val partitionedData = DataPartitionerS2.partitionData(spark, clusteringDataset, epsilon_partly_outside_range, S2_LVL)
    import spark.implicits._

    partitionedData.flatMapGroups((key, vals) => (vals.toList)).collect().foreach(println)

    partitionedData.keys.count() should equal(4)
    partitionedData.keys.collect() should contain theSameElementsAs (GEO_DATA_KEYS)
    partitionedData.flatMapGroups((key, vals) => vals.map(_.features.toArray).toList).collect() should contain theSameElementsAs (GEO_DATA_VALUES)
  }
  it should "replicate instances to reachable cells" in
  {
    val partitionedData = DataPartitionerS2.partitionData(spark, complexDataset, epsilon_partly_outside_range, S2_LVL)
    import spark.implicits._

    val collectedPartitionedData = partitionedData.flatMapGroups{case(key, vals) => (vals.map(instance => (key,instance)))}.collect()

    collectedPartitionedData.foreach(println)
    collectedPartitionedData.map(_._1).distinct.size should equal (5)

    //    //neviim cell
    //    (1521455267617701888,ClusteringInstance(1,0,false,4,(34.778023,32.073889),[6.0,7.0]))
    //    (1521455267617701888,ClusteringInstance(1,0,false,4,(34.778137,32.074229),[10.0,11.0]))
    collectedPartitionedData.filter(_._1==1521455267617701888L).size should equal (2)
    collectedPartitionedData.filter(_._1==1521455267617701888L).map(_._2.features(0)) should
      contain theSameElementsAs (List(6.0, 10.0))
    //    //voodo cell
    //    (1521455263322734592,ClusteringInstance(1,0,false,4,(34.778023,32.073889),[6.0,7.0]))
    //    (1521455263322734592,ClusteringInstance(1,0,false,4,(34.775628,32.074032),[5.0,5.0]))
    //    (1521455263322734592,ClusteringInstance(1,0,false,4,(34.777112,32.0718015),[8.0,9.0]))
    //    (1521455263322734592,ClusteringInstance(1,0,false,4,(34.777547,32.072729),[11.0,2.0]))
    //    (1521455263322734592,ClusteringInstance(1,0,false,4,(34.777558,32.072565),[3.0,4.0]))
    collectedPartitionedData.filter(_._1==1521455263322734592L).size should equal (5)
    collectedPartitionedData.filter(_._1==1521455263322734592L).map(_._2.features(0)) should
      contain theSameElementsAs (List(3.0, 5.0, 6.0, 8.0, 11.0))
    //    // Khisin cell
    //    (1521455269765185536,ClusteringInstance(1,0,false,4,(34.778023,32.073889),[6.0,7.0]))
    //    (1521455269765185536,ClusteringInstance(1,0,false,4,(34.778137,32.074229),[10.0,11.0]))
    //    (1521455269765185536,ClusteringInstance(1,0,false,4,(34.777112,32.0718015),[8.0,9.0]))
    //    (1521455269765185536,ClusteringInstance(1,0,false,4,(34.777547,32.072729),[11.0,2.0]))
    //    (1521455269765185536,ClusteringInstance(1,0,false,4,(34.777558,32.072565),[3.0,4.0]))
    collectedPartitionedData.filter(_._1==1521455269765185536L).size should equal (5)
    collectedPartitionedData.filter(_._1==1521455269765185536L).map(_._2.features(0)) should
      contain theSameElementsAs (List(3.0, 6.0, 8.0, 10.0, 11.0))
    //    //Hashmonaim melchet cell
    //    (1521455261175250944,ClusteringInstance(1,0,false,4,(34.777112,32.0718015),[8.0,9.0]))
    //    (1521455261175250944,ClusteringInstance(1,0,false,4,(34.777547,32.072729),[11.0,2.0]))
    //    (1521455261175250944,ClusteringInstance(1,0,false,4,(34.777558,32.072565),[3.0,4.0]))
    collectedPartitionedData.filter(_._1==1521455261175250944L).size should equal (3)
    collectedPartitionedData.filter(_._1==1521455261175250944L).map(_._2.features(0)) should
      contain theSameElementsAs (List(3.0, 8.0, 11.0))
    //    //ahad ha'am cell
    //    (1521455259027767296,ClusteringInstance(1,0,false,4,(34.777112,32.0718015),[8.0,9.0]))
    //    (1521455259027767296,ClusteringInstance(1,0,false,4,(34.777547,32.072729),[11.0,2.0]))
    //    (1521455259027767296,ClusteringInstance(1,0,false,4,(34.777558,32.072565),[3.0,4.0]))
    collectedPartitionedData.filter(_._1==1521455259027767296L).size should equal (3)
    collectedPartitionedData.filter(_._1==1521455259027767296L).map(_._2.features(0)) should
      contain theSameElementsAs (List(3.0, 8.0, 11.0))
  }

}
