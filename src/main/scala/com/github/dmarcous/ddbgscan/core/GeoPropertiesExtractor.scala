package com.github.dmarcous.ddbgscan.core

import com.github.dmarcous.ddbgscan.model.{ClusteringInstance, KeyGeoEntity, LonLatGeoEntity}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, Dataset}

object GeoPropertiesExtractor {
  def fromLonLatDelimitedFile(
                               data: Dataset[String],
                               delimiter : String = ",",
                               positionLon: Int = 0, positionLat: Int = 1)
  : Dataset[(KeyGeoEntity, ClusteringInstance)] =
  {
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
          new ClusteringInstance(features)
        )
      }
  }

  def fromLonLatDataFrame(
                           data: DataFrame,
                           positionLon: Int = 0, positionLat: Int = 1)
  : Dataset[(KeyGeoEntity, ClusteringInstance)] =
  {
    data
      .map{case(fields) =>
        val lon = fields.getDouble(positionLon)
        val lat = fields.getDouble(positionLat)
        val features =
          Vectors.dense(
            fields.toSeq.toBuffer
              .drop(positionLon).drop(positionLat)
              .toArray[Double]
          )
        (lon, lat, features)
      }
      .map{case(lon, lat, features) =>
        (LonLatGeoEntity(lon, lat).asInstanceOf[KeyGeoEntity],
          new ClusteringInstance(features)
        )
      }
  }
}
