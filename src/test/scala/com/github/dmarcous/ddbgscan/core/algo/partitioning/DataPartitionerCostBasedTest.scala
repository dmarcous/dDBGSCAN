package com.github.dmarcous.ddbgscan.core.algo.partitioning

import com.github.dmarcous.ddbgscan.core.algo.partitioning.DataPartitionerCostBased.{DBSCANRectangle, RectangleWithCount, canBeSplit, findBoundingRectangle, findEvenSplitsPartitions, partition, pointsInRectangle, split, toMinimumBoundingRectangle}
import com.github.dmarcous.ddbgscan.core.config.CoreConfig.{DEFAULT_GEO_FILE_DELIMITER, DEFAULT_LATITUDE_POSITION_FIELD_NUMBER, DEFAULT_LONGITUDE_POSITION_FIELD_NUMBER, NO_UNIQUE_ID_FIELD}
import com.github.dmarcous.ddbgscan.core.config.IOConfig
import com.github.dmarcous.ddbgscan.core.preprocessing.GeoPropertiesExtractor
import com.github.dmarcous.s2utils.converters.UnitConverters
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class DataPartitionerCostBasedTest extends FlatSpec{

  val spark =
    SparkSession
      .builder()
      .master("local")
      .appName("DataPartitionerCostBasedTest")
      .getOrCreate()
  import spark.implicits._

  val S2_LVL = 15
  val maxPointsPerPartition=3
  val maxPointsPerPartition_minimal=1
  val epsilon = 40.0

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

  val complexDataset =
    GeoPropertiesExtractor.fromLonLatDelimitedFile(
      spark, complexLonLatDelimitedGeoData, S2_LVL, ioConfig
    )

  "partitionData" should "keep close together points in the same partition" in
  {
    val epsilon_partly_outside_range = 40.0
    val partitionedData = DataPartitionerCostBased.partitionData(spark, complexDataset, epsilon_partly_outside_range, maxPointsPerPartition)

    import spark.implicits._
    val collectedPartitionedData = partitionedData.flatMap{case(key, vals) => (vals.map(instance => (key,instance)))}.collect()

    collectedPartitionedData.foreach(println)
    collectedPartitionedData.map(_._1).distinct.size should equal(2)
  }
  it should "keep a single point per partition if asked" in
  {
    val epsilon_minimal = 1.0
    val partitionedData = DataPartitionerCostBased.partitionData(spark, complexDataset, epsilon_minimal, maxPointsPerPartition_minimal)

    import spark.implicits._
    val collectedPartitionedData = partitionedData.flatMap{case(key, vals) => (vals.map(instance => (key,instance)))}.collect()

    collectedPartitionedData.foreach(println)
    collectedPartitionedData.map(_._1).distinct.size should equal(6)
  }

//  // DEBUG Code
//  "toMinimumBoundingRectangle" should "group by MBRs in given range" in
//    {
//      import spark.implicits._
//
//      val maxPointsPerPartition=1000
//      val epsilon = 300.0
//
//      val minimumRectangleSize = UnitConverters.metricToAngularDistance(epsilon * 2)
//      val geo = "./src/test/resources/sampleInputs/geo005.csv"
//      val ds =
//        GeoPropertiesExtractor.fromLonLatDelimitedFile(
//          spark,
//          spark.read.textFile(geo),
//          S2_LVL,
//          IOConfig(
//            inputPath,
//            outputFolderPath,
//            0,
//            1,
//            2,
//            delimiter,
//            numPartitions
//          )
//        )
//
//      println("ds : " + ds.count())
//
//      val minimumRectanglesWithCount =
//        ds
//          .map{case(key, inst) => ((DataPartitionerCostBased.toMinimumBoundingRectangle(inst.lonLatLocation, minimumRectangleSize),1))}
//          .groupByKey(_._1)
//          .count()
//
//      //    val localPartitions =
//      //      DataPartitionerCostBased.findEvenSplitsPartitions(
//      //        minimumRectanglesWithCount, 999, minimumRectangleSize)
//
//      val boundingRectangle = DataPartitionerCostBased.findBoundingRectangle(minimumRectanglesWithCount)
//      val boundingSet = minimumRectanglesWithCount.collect().toSet
//
//      def pointsIn = pointsInRectangle(boundingSet, _: DBSCANRectangle)
//
//      val toPartition = List((boundingRectangle, pointsIn(boundingRectangle)))
//      val partitioned = List[RectangleWithCount]()
//
//      def partition(
//                     remaining: List[RectangleWithCount],
//                     partitioned: List[RectangleWithCount],
//                     pointsIn: (DBSCANRectangle) => Long,
//                     minimumRectangleSize: Double,
//                     maxPointsPerPartition: Long): List[RectangleWithCount] =
//      {
//        remaining match {
//          case (rectangle, count) :: rest =>
//            if (count > maxPointsPerPartition) {
//              println("count=" + count.toString + " > maxPointsPerPartition=" + maxPointsPerPartition)
//
//              if (canBeSplit(rectangle, minimumRectangleSize)) {
//                println("Can be split = True")
//
//                def cost = (r: DBSCANRectangle) => ((pointsIn(rectangle) / 2) - pointsIn(r)).abs
//                val (split1, split2) = split(rectangle, cost, minimumRectangleSize)
//                val s1 = (split1, pointsIn(split1))
//                val s2 = (split2, pointsIn(split2))
//                partition(s1 :: s2 :: rest, partitioned, pointsIn, minimumRectangleSize, maxPointsPerPartition)
//
//              } else {
//                println("Can be split = False")
//                partition(rest, (rectangle, count) :: partitioned, pointsIn, minimumRectangleSize, maxPointsPerPartition)
//              }
//
//            } else {
//              println("count=" + count.toString + " <= maxPointsPerPartition=" + maxPointsPerPartition)
//              partition(rest, (rectangle, count) :: partitioned, pointsIn, minimumRectangleSize, maxPointsPerPartition)
//            }
//
//          case Nil => partitioned
//
//        }
//      }
//
//      val partitions = partition(toPartition, partitioned, pointsIn, minimumRectangleSize, maxPointsPerPartition)
//      val localPartitions = partitions.filter({ case (partition, count) => count > 0 })
//
//      //    println("minimumRectanglesWithCount")
//      //    minimumRectanglesWithCount.collect().foreach(println)
//      println("boundingRectangle : " + boundingRectangle.toString)
//      //    println("boundingSet : " + boundingSet.toString)
//      println("toPartition : " + toPartition.toString)
//      println("partitions")
//      partitions.foreach(println)
//      println("localPartitions")
//      localPartitions.foreach(println)
//
//    }

}
