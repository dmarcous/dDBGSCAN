package com.github.dmarcous.ddbgscan.core.config

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

  // Partitioning strategy
  val GEO_PARTITIONING_STRATEGY = "S2"
  val COST_PARTITIONING_STRATEGY = "cost"
  val SUPPORTED_PARTITIONING_STRATEGIES = List(GEO_PARTITIONING_STRATEGY, COST_PARTITIONING_STRATEGY)

  // NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTIONS
  val DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION = (x: Vector, y: Vector) => true
  val GEO_DISTANCE_UNDER_500m = (x: Vector, y: Vector) => {
    GeographyUtilities.haversineDistance(x.asInstanceOf[Point], y.asInstanceOf[Point]) < 500
  }

  val NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION_TRANSLATOR =
    HashMap[Int, (Vector, Vector) => Boolean](
        DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION_CODE -> DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION,
        GEO_DISTANCE_UNDER_500m_SIMILARITY_EXTENSION_FUNCTION_CODE -> GEO_DISTANCE_UNDER_500m
        )
    .withDefaultValue(DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION)

  // Tuning parameters
  val DEFAULT_NUM_PARTITIONS = 256

  // Clustering instance properties
  val UNKNOWN_CLUSTER = -1L

  sealed trait ClusteringInstanceStatus {
    def value: Int
  }
  object ClusteringInstanceStatus {
    def apply(int: Int): ClusteringInstanceStatus = {
      int match {
        case ClusteringInstanceStatusValue.CORE.value => ClusteringInstanceStatusValue.CORE
        case ClusteringInstanceStatusValue.BORDER.value => ClusteringInstanceStatusValue.BORDER
        case ClusteringInstanceStatusValue.NOISE.value => ClusteringInstanceStatusValue.NOISE
        case ClusteringInstanceStatusValue.UNKNOWN.value => ClusteringInstanceStatusValue.UNKNOWN
        case _ => throw new IllegalArgumentException
      }
    }

    def unapply(clusteringInstanceStatus: ClusteringInstanceStatus): Int =
      clusteringInstanceStatus.value
  }
  object ClusteringInstanceStatusValue {
    final case object CORE extends ClusteringInstanceStatus {
      val value = 1
    }
    final case object BORDER extends ClusteringInstanceStatus {
      val value = 2
    }
    final case object NOISE extends ClusteringInstanceStatus {
      val value = 3
    }
    final case object UNKNOWN extends ClusteringInstanceStatus {
      val value = 4
    }
  }

  // Dataset parameters defaults
  val NO_UNIQUE_ID_FIELD = -1
  val DEFAULT_LONGITUDE_POSITION_FIELD_NUMBER = 0
  val DEFAULT_LATITUDE_POSITION_FIELD_NUMBER = 1

  val DEFAULT_RECORD_ID = 1L

  // Default delimiters
  val DEFAULT_GEO_FILE_DELIMITER = ","

  // Default debug
  val DEFAULT_DEBUG = false
}
