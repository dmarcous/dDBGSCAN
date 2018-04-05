package com.github.dmarcous.ddbgscan.core

import com.github.dmarcous.s2utils.geo.GeographyUtilities
import com.vividsolutions.jts.geom.Point

import scala.collection.immutable.HashMap

object CoreConfig {
  // Algorithm config defaults
  val MISSING_NEIGHBORHOOD_LVL = -1

  // NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTIONS
  val DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION = (x: Any, y: Any) => true
  val GEO_DISTANCE_UNDER_500m = (x: Any, y: Any) => {
    GeographyUtilities.haversineDistance(x.asInstanceOf[Point], y.asInstanceOf[Point]) < 500
  }

  // Internal algorithm default helpers
  val SMALLEST_CELL_AREA_EPSILON_MULTIPLIER = 16

  // Extendable similarity functions
  val NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION_TRANSLATOR : Map[Int, (Any, Any) => Boolean] =
    HashMap(
      0 -> DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION,
      1 -> GEO_DISTANCE_UNDER_500m
    )
    .withDefaultValue(DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION)

  // Clustering instance properties
  val UNKNOWN_CLUSTER = 0L
  object ClusteringInstanceStatus extends Enumeration {
    val CORE = Value
    val BORDER = Value
    val NOISE = Value
    val UNKNOWN = Value
  }
}
