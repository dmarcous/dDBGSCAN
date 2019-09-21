package com.github.dmarcous.ddbgscan.core.algo.clustering

import java.util.ArrayList

import com.github.davidmoten.grumpy.core.Position
import com.github.davidmoten.rtree.geometry.{Geometries, Point, Rectangle}
import com.github.davidmoten.rtree.{Entries, Entry, RTree}
import com.github.dmarcous.ddbgscan.model.ClusteringInstance
import rx.functions.Func1
import scala.math.{min,max}

import scala.collection.JavaConverters._

object PointSearchUtilities {

  def buildPointGeometrySearchTree(instances: List[ClusteringInstance]) : RTree[ClusteringInstance, Point] =
  {
    val entryList =
      instances.map(instance =>
        Entries.entry(instance,
          Geometries.pointGeographic(
            instance.lonLatLocation._1,
            instance.lonLatLocation._2))).asJava
    // Adding to ArrayList for all functionality (otherwise getting array backed java list)
    val searchTree = RTree.star().create[ClusteringInstance, Point](new ArrayList(entryList))

    searchTree
  }

  def getClusteringInstancesFromSearchTree(tree: RTree[ClusteringInstance, Point]): List[ClusteringInstance] =
  {
    tree.entries().toBlocking().toIterable.asScala.map{case(entry) => entry.value()}.toList
  }

  // epsilon = meters
  def getGeoDensityReachablePointsFromSearchTree(tree: RTree[ClusteringInstance, Point], lonLat: Point, epsilon: Double) : List[Entry[ClusteringInstance, Point]] =
  {
    val reachingDistanceKM : Double = (epsilon + 0.1)/1000.0 // Add buffer to epsilon to get all points

    // First we need to calculate an enclosing lat long rectangle for this
    // distance then we refine on the exact distance
    val from = Position.create(lonLat.y, lonLat.x)
    val bounds = createBounds(from, reachingDistanceKM)

    //return
    val reachableInstances =
      tree
        .search(bounds) // do the first search using the bounds
        .filter( // refine using the exact distance
          new Func1[Entry[ClusteringInstance, Point], java.lang.Boolean]
          {
            override def call(entry: Entry[ClusteringInstance, Point]): java.lang.Boolean =
            {
              val p : Point = entry.geometry()
              val position : Position = Position.create(p.y, p.x)
              val inRange : java.lang.Boolean = (from.getDistanceToKm(position) < reachingDistanceKM)
              return inRange
            }
          }
        )

    reachableInstances.toBlocking.toIterable.asScala.toList
  }

  // Tested in https://github.com/davidmoten/rtree/blob/master/src/test/java/com/github/davidmoten/rtree/LatLongExampleTest.java
  // this calculates a pretty accurate bounding box. Depending on the
  def createBounds(from: Position, distanceKm: Double) : Rectangle =
  {
      // performance you require you wouldn't have to be this accurate because
      // accuracy is enforced later
      val north = from.predict(distanceKm, 0)
      val south = from.predict(distanceKm, 180)
      val east = from.predict(distanceKm, 90)
      val west = from.predict(distanceKm, 270)
      // validation for edge cases :
      val x1 = min(west.getLon, east.getLon)
      val x2 = max(west.getLon, east.getLon)
      val y1 = min(south.getLat, north.getLat)
      val y2 = max(south.getLat, north.getLat)
      Geometries.rectangle(x1, y1, x2, y2)
    }
}
