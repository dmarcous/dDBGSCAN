package com.github.dmarcous.ddbgscan.core.algo.partitioning

import com.github.dmarcous.ddbgscan.model.{ClusteringInstance, KeyGeoEntity}
import com.github.dmarcous.s2utils.converters.UnitConverters
import com.github.dmarcous.s2utils.geo.GeographyUtilities
import com.vividsolutions.jts.geom.{Coordinate, Envelope}
import org.apache.spark.sql.{Dataset, SparkSession}
import scala.annotation.tailrec

// Adapted to geo aware cost based partitioning from https://github.com/irvingc/dbscan-on-spark
object DataPartitionerCostBased {

  def partitionData(@transient spark: SparkSession,
                    data: Dataset[(KeyGeoEntity, ClusteringInstance)],
                    epsilon: Double,
                    maxPointsPerPartition : Int
         ): Dataset[(Long, List[ClusteringInstance])] =
  {
    import spark.implicits._

    val minimumRectangleSize = UnitConverters.metricToAngularDistance(epsilon)*2

    val minimumRectanglesWithCount =
      data
      .map{case(key, inst) => ((toMinimumBoundingRectangle(inst.lonLatLocation, epsilon),1))}
      .groupByKey(_._1)
      .count()

    // find the best partitions for the data space
    val localPartitions =
      findEvenSplitsPartitions(
        minimumRectanglesWithCount, maxPointsPerPartition, minimumRectangleSize)

    // grow partitions to include eps
    val localMargins =
      localPartitions
        .map({ case (p, _) => (shrinkRectangle(p,epsilon), p, shrinkRectangle(p,-epsilon)) })
        .zipWithIndex
    val margins = spark.sparkContext.broadcast(localMargins)

    // assign each point to its proper partition
    val duplicated = for {
      (inst) <- data.rdd.map{case(key, inst) => (inst)}
      ((inner, main, outer), id) <- margins.value
      if rectangleContainsPoint(outer,inst.lonLatLocation)
    } yield (id, inst)

    val repartitionedData =
      duplicated
      .map{case(key, inst) => (key.toLong, inst)}
      .toDS()
      .groupByKey(_._1) // group by partition key
      .mapValues(_._2) // remove redundant inner id
      // Sum number of inner instances
      .mapGroups{case(cellId,instances)=>
        (cellId,instances.toList)}
      .repartitionByRange(partitionExprs=$"_1") // Partition by cell id

    // Trigger shuffle for repartitioning to happen
    repartitionedData.count()

    repartitionedData
  }

  private def toMinimumBoundingRectangle(coordinate: (Double, Double), eps: Double): (Double, Double, Double, Double) = {
    val x = coordinate._1
    val y = coordinate._2
    getBoundingBox(x, y, eps)
  }

  private def getBoundingBox(x: Double, y: Double, radiusMeters: Double): (Double, Double, Double, Double) = {
    val gf = GeographyUtilities.createGeometryFactory()
    val angleRadius = UnitConverters.metricToAngularDistance(radiusMeters)

    val boundingBox =
      gf.createPoint(new Coordinate(x, y))
        .buffer(angleRadius).getEnvelopeInternal

    rectangleFromGeoBoundingBoxEnvelope(boundingBox)
  }

  private def geoDistance(self: (Double, Double), other: (Double, Double)): Double = {
    GeographyUtilities.haversineDistance(self._1, self._2, other._1, other._2)
  }

  private def rectangleFromGeoBoundingBoxEnvelope(bb: Envelope): (Double, Double, Double, Double) = {
    (bb.getMinX, bb.getMinY, bb.getMaxX, bb.getMaxY)
  }

  /**
   * Returns whether other is contained by this box
   */
  private def rectangleContainsRectangle(self: (Double, Double, Double, Double), other: (Double, Double, Double, Double)): Boolean = {
    self._1 <= other._1 && other._3 <= self._3 && self._2 <= other._2 && other._4 <= self._4
  }

  /**
   * Returns whether point is contained by this box
   */
  private def rectangleContainsPoint(self: (Double, Double, Double, Double), point: (Double, Double)): Boolean = {
    self._1 <= point._1 && point._1 <= self._3 && self._2 <= point._2 && point._2 <= self._4
  }

  /**
   * Returns a new box from shrinking this box by the given amount
   */
  private def shrinkRectangle(self: (Double, Double, Double, Double), amountMeters: Double): (Double, Double, Double, Double) = {
    val angleRadius = UnitConverters.metricToAngularDistance(amountMeters)

    var bb = rectangleToGeoBoundingBoxEnvelope(self)
    bb.expandBy(-1.0 * angleRadius)

    rectangleFromGeoBoundingBoxEnvelope(bb)
  }

  private def rectangleToGeoBoundingBoxEnvelope(self: (Double, Double, Double, Double)): Envelope = {
    val gf = GeographyUtilities.createGeometryFactory()
    var boundingBox =
      gf.createPoint(new Coordinate(self._1, self._2))
        .getEnvelopeInternal

    boundingBox.expandToInclude(new Coordinate(self._3, self._4))

    boundingBox
  }

  /**
   * Returns a whether the rectangle contains the point, and the point
   * is not in the rectangle's border
   */
  private def rectangleAlmostContains(self: (Double, Double, Double, Double), point: (Double, Double)): Boolean = {
    self._1 < point._1 && point._1 < self._3 && self._2 < point._2 && point._2 < self._4
  }


  //case class DBSCANGeoPoint(x, y)
  //case class DBSCANRectangle(x: Double, y: Double, x2: Double, y2: Double)
  //type RectangleWithCount = ((Double, Double, Double, Double), Long)
  private type DBSCANRectangle = (Double, Double, Double, Double)
  private type RectangleWithCount = ((Double, Double, Double, Double), Long)

  private def findEvenSplitsPartitions(toSplit: Dataset[((Double, Double, Double, Double), Long)],
                               maxPointsPerPartition: Long,
                               minimumRectangleSize: Double): List[((Double, Double, Double, Double), Long)] = {

    val boundingRectangle = findBoundingRectangle(toSplit)
    val boundingSet = toSplit.collect().toSet

    def pointsIn = pointsInRectangle(boundingSet, _: DBSCANRectangle)

    val toPartition = List((boundingRectangle, pointsIn(boundingRectangle)))
    val partitioned = List[RectangleWithCount]()

    val partitions = partition(toPartition, partitioned, pointsIn, minimumRectangleSize, maxPointsPerPartition)

    // remove empty partitions
    partitions.filter({ case (partition, count) => count > 0 })

  }

  @tailrec
  private def partition(
                         remaining: List[RectangleWithCount],
                         partitioned: List[RectangleWithCount],
                         pointsIn: (DBSCANRectangle) => Long,
                         minimumRectangleSize: Double,
                         maxPointsPerPartition: Long): List[RectangleWithCount] = {

    remaining match {
      case (rectangle, count) :: rest =>
        if (count > maxPointsPerPartition) {

          if (canBeSplit(rectangle, minimumRectangleSize)) {
            def cost = (r: DBSCANRectangle) => ((pointsIn(rectangle) / 2) - pointsIn(r)).abs
            val (split1, split2) = split(rectangle, cost, minimumRectangleSize)
            val s1 = (split1, pointsIn(split1))
            val s2 = (split2, pointsIn(split2))
            partition(s1 :: s2 :: rest, partitioned, pointsIn, minimumRectangleSize, maxPointsPerPartition)

          } else {
            partition(rest, (rectangle, count) :: partitioned, pointsIn, minimumRectangleSize, maxPointsPerPartition)
          }

        } else {
          partition(rest, (rectangle, count) :: partitioned, pointsIn, minimumRectangleSize, maxPointsPerPartition)
        }

      case Nil => partitioned

    }

  }

  private def split(
             rectangle: DBSCANRectangle,
             cost: (DBSCANRectangle) => Long,
             minimumRectangleSize: Double): (DBSCANRectangle, DBSCANRectangle) = {

    val smallestSplit =
      findPossibleSplits(rectangle, minimumRectangleSize)
        .reduceLeft {
          (smallest, current) =>

            if (cost(current) < cost(smallest)) {
              current
            } else {
              smallest
            }

        }

    (smallestSplit, (complement(smallestSplit, rectangle)))

  }

  /**
   * Returns the box that covers the space inside boundary that is not covered by box
   */
  private def complement(box: DBSCANRectangle, boundary: DBSCANRectangle): DBSCANRectangle =
    if (box._1 == boundary._1 && box._2 == boundary._2) {
      if (boundary._3 >= box._3 && boundary._4 >= box._4) {
        if (box._4 == boundary._4) {
          (box._3, box._2, boundary._3, boundary._4)
        } else if (box._3 == boundary._3) {
          (box._1, box._4, boundary._3, boundary._4)
        } else {
          throw new IllegalArgumentException("rectangle is not a proper sub-rectangle")
        }
      } else {
        throw new IllegalArgumentException("rectangle is smaller than boundary")
      }
    } else {
      throw new IllegalArgumentException("unequal rectangle")
    }

  /**
   * Returns all the possible ways in which the given box can be split
   */
  private def findPossibleSplits(box: DBSCANRectangle, minimumRectangleSize: Double): Set[DBSCANRectangle] = {

    val xSplits = (box._1 + minimumRectangleSize) until box._3 by minimumRectangleSize

    val ySplits = (box._2 + minimumRectangleSize) until box._4 by minimumRectangleSize

    val splits =
      xSplits.map(x => (box._1, box._2, x, box._4)) ++
        ySplits.map(y => (box._1, box._2, box._3, y))

    splits.toSet
  }

  /**
   * Returns true if the given rectangle can be split into at least two rectangles of minimum size
   */
  private def canBeSplit(box: DBSCANRectangle, minimumRectangleSize: Double): Boolean = {
    (box._3 - box._1 > minimumRectangleSize * 2 ||
      box._4 - box._2 > minimumRectangleSize * 2)
  }

  private def pointsInRectangle(space: Set[RectangleWithCount], rectangle: DBSCANRectangle): Long = {
    space.view
      .filter({ case (current, _) => rectangleContainsRectangle(rectangle, current) })
      .foldLeft(0L) {
        case (total, (_, count)) => total + count
      }
  }

  private def findBoundingRectangle(rectanglesWithCount: Dataset[((Double, Double, Double, Double), Long)]): (Double, Double, Double, Double) = {

    val invertedRectangle =
      (Double.MaxValue, Double.MaxValue, Double.MinValue, Double.MinValue)

    rectanglesWithCount
      .rdd
      .map{case(bb, counter) => bb}
      .fold(invertedRectangle) {
      case (bounding, (c)) =>
        (
          bounding._1.min(c._1), bounding._2.min(c._2),
          bounding._3.max(c._3), bounding._4.max(c._4))
    }

  }

}
