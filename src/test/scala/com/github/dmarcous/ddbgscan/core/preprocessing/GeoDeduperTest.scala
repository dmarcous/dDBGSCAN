package com.github.dmarcous.ddbgscan.core.preprocessing

import com.github.dmarcous.ddbgscan.core.config.CoreConfig._
import com.github.dmarcous.ddbgscan.core.config.{AlgorithmParameters, IOConfig}
import com.github.dmarcous.ddbgscan.model.{ClusteredInstance, ClusteringInstance, KeyGeoEntity}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class GeoDeduperTest extends FlatSpec{

  val spark =
    SparkSession
      .builder()
      .master("local")
      .appName("GeoDeduperTest")
      .getOrCreate()
  import spark.implicits._

  val S2_LVL = 15

  val lonLatGeoDataframe =
    spark.createDataFrame(
      spark.sparkContext.makeRDD(
        List(
          Row(34.770112,32.0718015,1,2,3,4,5),
          Row(34.7747174,32.0724609,6,7,8,9,10)
        )),
      StructType(List(
        StructField("lon", DoubleType, false),
        StructField("lat", DoubleType, false),
        StructField("ft1", IntegerType, false),
        StructField("ft2", IntegerType, false),
        StructField("ft3", IntegerType, false),
        StructField("ft4", IntegerType, false),
        StructField("ft5", IntegerType, false))
      )
    )

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

  "dedupByInstanceGeo" should "return all instances when not active" in
    {
      val GEO_SENSITIVITY = MISSING_GEO_DECIMAL_SENSITIVITY_LVL
      val parameters = createAlgorithmParametersStruct(GEO_SENSITIVITY)

      val clusteringDataset =
        GeoPropertiesExtractor.fromLonLatDataFrame(
          spark, lonLatGeoDataframe, GEO_SENSITIVITY, S2_LVL, ioConfig
        )

      val dedupedDataset =
        GeoDeduper.dedupByInstanceGeo(
          spark, clusteringDataset, parameters
        )

      dedupedDataset.collect().foreach(println)

      dedupedDataset.count() should equal(2)
      dedupedDataset.map(_._1.geoData).collect() should equal (Array(Array("34.770112", "32.0718015"), Array("34.7747174", "32.0724609")))
      dedupedDataset.map(_._2.lonLatLocation).collect() should equal (Array((34.770112,32.0718015), (34.7747174,32.0724609)))
    }
  it should "return all instances when deduped to high precision" in
    {
      val GEO_SENSITIVITY = 4
      val parameters = createAlgorithmParametersStruct(GEO_SENSITIVITY)

      val clusteringDataset =
        GeoPropertiesExtractor.fromLonLatDataFrame(
          spark, lonLatGeoDataframe, GEO_SENSITIVITY, S2_LVL, ioConfig
        )

      val dedupedDataset =
        GeoDeduper.dedupByInstanceGeo(
          spark, clusteringDataset, parameters
        )

      dedupedDataset.collect().foreach(println)

      dedupedDataset.count() should equal(2)
      dedupedDataset.map(_._1.geoData).collect() should equal (Array(Array("34.7747174", "32.0724609"), Array("34.770112", "32.0718015")))
      dedupedDataset.map(_._2.lonLatLocation).collect() should equal (Array((34.7747,32.0725), (34.7701,32.0718)))
    }
  it should "dedup instances when deduped to low precision" in
    {
      val GEO_SENSITIVITY = 2
      val parameters = createAlgorithmParametersStruct(GEO_SENSITIVITY)

      val clusteringDataset =
        GeoPropertiesExtractor.fromLonLatDataFrame(
          spark, lonLatGeoDataframe, GEO_SENSITIVITY, S2_LVL, ioConfig
        )

      val dedupedDataset =
        GeoDeduper.dedupByInstanceGeo(
          spark, clusteringDataset, parameters
        )

      dedupedDataset.collect().foreach(println)

      dedupedDataset.count() should equal(1)
      dedupedDataset.map(_._1.geoData).collect() should equal (Array(Array("34.770112", "32.0718015")))
      dedupedDataset.map(_._2.lonLatLocation).collect() should equal (Array((34.77,32.07)))
    }

  "restoreDupesToClusters" should "...." in
    {
      val customLonLatDelimitedGeoData =
        spark.createDataset(
          Seq(
            "1,34.7000001,32.7000001,1,2,3,4,5",
            "2,34.7000002,32.7000002,6,7,8,9,10",
            "3,34.7000003,32.7000003,6,7,8,9,10",
            "4,34.7100000,32.7100000,6,7,8,9,10",
            "5,40.7747174,32.0774609,6,7,8,9,10",
            "6,41.7747174,32.0774609,6,7,8,9,10"
          ))
      val customPositionId = 0
      val customPositionLon = 1
      val customPositionLat = 2
      val customIOConfig = IOConfig(
        inputPath,
        outputFolderPath,
        customPositionId,
        customPositionLon,
        customPositionLat,
        delimiter
      )
      val GEO_SENSITIVITY = 2
      val parameters = createAlgorithmParametersStruct(GEO_SENSITIVITY)
      val originalData =
        GeoPropertiesExtractor.fromLonLatDelimitedFile(
          spark, customLonLatDelimitedGeoData, GEO_SENSITIVITY, S2_LVL, customIOConfig
        )

      val globallyClusteredData =
        spark.createDataFrame(
          spark.sparkContext.makeRDD(
            List(
              Row(1L, 1L, ClusteringInstanceStatusValue.CORE.value),
              Row(4L, 1L, ClusteringInstanceStatusValue.CORE.value),
              Row(5L, 2L, ClusteringInstanceStatusValue.CORE.value),
              Row(6L, 3L, ClusteringInstanceStatusValue.CORE.value)
            )),
          StructType(List(
            StructField("recordId", LongType, false),
            StructField("cluster", LongType, false),
            StructField("instanceStatus", IntegerType, false)
          ))
        ).as[ClusteredInstance]

      val restoredDataset =
        GeoDeduper.restoreDupesToClusters(
          spark, globallyClusteredData, originalData
        )

      restoredDataset.collect().foreach(println)
      restoredDataset.count() should equal(6)
      restoredDataset.sort("recordId").map(_.recordId).collect() should equal (Array(1L,2L,3L,4L,5L,6L))
      restoredDataset.sort("recordId").map(_.cluster).collect() should equal (Array(1L,1L,1L,1L,2L,3L))

    }

  private def createAlgorithmParametersStruct(geoSensitivity: Int): AlgorithmParameters =
    AlgorithmParameters(
      0.0,
      0,
      GEO_PARTITIONING_STRATEGY,
      geoSensitivity,
      S2_LVL,
      DEFAULT_NUM_PARTITIONS,
      DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION
    )

}
