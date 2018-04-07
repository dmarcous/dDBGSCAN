package com.github.dmarcous.ddbgscan.core

import com.github.dmarcous.ddbgscan.core.CoreConfig.DEFAULT_GEO_FILE_DELIMITER
import com.github.dmarcous.ddbgscan.model.{ClusteringInstance, KeyGeoEntity, LonLatGeoEntity}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object GeoPropertiesExtractor {
  def fromLonLatDelimitedFile(@transient spark: SparkSession,
                               data: Dataset[String],
                               delimiter : String = DEFAULT_GEO_FILE_DELIMITER,
                               positionLon: Int = 0, positionLat: Int = 1)
  : Dataset[(KeyGeoEntity, ClusteringInstance)] =
  {
    import spark.implicits._

    data
      .map{case(line) => line.split(delimiter)}
      .map{case(fields) =>
        val lon = fields(positionLon).toDouble
        val lat = fields(positionLat).toDouble
        val features =
          Vectors.dense(
          fields.toBuffer
            .drop(positionLon).drop(positionLat)
            .toArray
            .map(_.toDouble))
        (lon, lat, features)
      }
      .map{case(lon, lat, features) =>
        (LonLatGeoEntity(lon, lat).asInstanceOf[KeyGeoEntity],
          new ClusteringInstance(features = features)
        )
      }
  }

  def fromLonLatDataFrame(@transient spark: SparkSession,
                           data: DataFrame,
                           positionLon: Int = 0, positionLat: Int = 1)
  : Dataset[(KeyGeoEntity, ClusteringInstance)] =
  {
    import spark.implicits._

    data
      .map{case(fields) =>
        val lon = fields.getDouble(positionLon)
        val lat = fields.getDouble(positionLat)
        val features =
          Vectors.dense(
            fields
              .mkString(DEFAULT_GEO_FILE_DELIMITER)
              .split(DEFAULT_GEO_FILE_DELIMITER)
              .toBuffer
              .drop(positionLon).drop(positionLat)
              .toArray
              .map(_.toDouble))
        (lon, lat, features)
      }
      .map{case(lon, lat, features) =>
        (LonLatGeoEntity(lon, lat).asInstanceOf[KeyGeoEntity],
          new ClusteringInstance(features = features)
        )
      }
  }
}
