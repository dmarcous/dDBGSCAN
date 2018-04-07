package com.github.dmarcous.ddbgscan.core

import com.github.dmarcous.s2utils.geo.GeographyUtilities
import com.vividsolutions.jts.geom.Point
import org.apache.spark.ml.linalg.Vector

import scala.collection.immutable.HashMap

object CoreConfig {
  // Algorithm config defaults
  val MISSING_NEIGHBORHOOD_LVL = -1

  // Internal algorithm default helpers
  val SMALLEST_CELL_AREA_EPSILON_MULTIPLIER = 16

  // Extendable similarity functions
  val DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION_CODE = 0
  val GEO_DISTANCE_UNDER_500m_SIMILARITY_EXTENSION_FUNCTION_CODE = 1

  // NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTIONS
  val DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION = (x: Vector, y: Vector) => true
  val GEO_DISTANCE_UNDER_500m = (x: Vector, y: Vector) => {
    GeographyUtilities.haversineDistance(x.asInstanceOf[Point], y.asInstanceOf[Point]) < 500
  }

//  private val initial_function_translator : HashMap[Int, (Vector, Vector) => Boolean] =
//    new HashMap[Int, (Vector, Vector) => Boolean](){ override def default(key:Int) = DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION }
  val NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION_TRANSLATOR =
//    initial_function_translator + (
HashMap[Int, (Vector, Vector) => Boolean](
    DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION_CODE -> DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION,
    GEO_DISTANCE_UNDER_500m_SIMILARITY_EXTENSION_FUNCTION_CODE -> GEO_DISTANCE_UNDER_500m
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

  // Default delimiters
  val DEFAULT_GEO_FILE_DELIMITER = ","
}
