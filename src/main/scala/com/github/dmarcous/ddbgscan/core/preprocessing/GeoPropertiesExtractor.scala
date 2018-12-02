package com.github.dmarcous.ddbgscan.core.preprocessing

import com.github.dmarcous.ddbgscan.core.config.CoreConfig.{DEFAULT_GEO_FILE_DELIMITER, DEFAULT_RECORD_ID, NO_UNIQUE_ID_FIELD}
import com.github.dmarcous.ddbgscan.core.config.IOConfig
import com.github.dmarcous.ddbgscan.model.{ClusteringInstance, KeyGeoEntity, LonLatGeoEntity}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.monotonically_increasing_id

object GeoPropertiesExtractor {

  def fromLonLatDelimitedFile(@transient spark: SparkSession,
                              data: Dataset[String],
                              neighborhoodPartitioningLvl: Int,
                              ioConfig : IOConfig)
  : Dataset[(KeyGeoEntity, ClusteringInstance)] =
  {
    import spark.implicits._

    val parsedData = data
      .map{case(line) => line.split(ioConfig.inputDelimiter)}
      .map{case(fields) =>
        val id = if(ioConfig.positionId == NO_UNIQUE_ID_FIELD) DEFAULT_RECORD_ID  else fields(ioConfig.positionId).toLong
        val lon = fields(ioConfig.positionLon).toDouble
        val lat = fields(ioConfig.positionLat).toDouble
        val fieldsToRemovePositions = List(ioConfig.positionId, ioConfig.positionLon, ioConfig.positionLat).sorted
        val featureFields =
          fields.zipWithIndex
          .filter{case(field, pos) => !fieldsToRemovePositions.contains(pos)}
          .map(_._1)
        val features =
          Vectors.dense(
            featureFields
            .map(_.toDouble))
        (id, lon, lat, features)
      }

    val identifiedData =
      if(ioConfig.positionId == NO_UNIQUE_ID_FIELD)
      {
        parsedData
          .toDF("id", "lon", "lat", "features")
          .withColumn("id",monotonically_increasing_id())
          .as[(Long, Double, Double, Vector)]
      }
      else
      {
        parsedData
      }

    identifiedData
      .map{case(id, lon, lat, features) =>
        (new KeyGeoEntity(LonLatGeoEntity(lon, lat), neighborhoodPartitioningLvl),
          ClusteringInstance(recordId = id, lonLatLocation= (lon, lat), features = features)
        )
      }
  }

  def fromLonLatDataFrame(@transient spark: SparkSession,
                          data: DataFrame,
                          neighborhoodPartitioningLvl: Int,
                          ioConfig : IOConfig)
  : Dataset[(KeyGeoEntity, ClusteringInstance)] =
  {
    import spark.implicits._

    val parsedData = data
      .map{case(fields) =>
        val id = if(ioConfig.positionId == NO_UNIQUE_ID_FIELD) DEFAULT_RECORD_ID else fields.getLong(ioConfig.positionId)
        val lon = fields.getDouble(ioConfig.positionLon)
        val lat = fields.getDouble(ioConfig.positionLat)
        val fieldsToRemovePositions = List(ioConfig.positionId, ioConfig.positionLon, ioConfig.positionLat).sorted
        val featureFields =
          fields.mkString(DEFAULT_GEO_FILE_DELIMITER)
            .split(DEFAULT_GEO_FILE_DELIMITER)
            .zipWithIndex
            .filter{case(field, pos) => !fieldsToRemovePositions.contains(pos)}
            .map(_._1)
        val features =
          Vectors.dense(
            featureFields
            .map(_.toDouble))
        (id, lon, lat, features)
      }

    val identifiedData =
      if(ioConfig.positionId == NO_UNIQUE_ID_FIELD)
      {
        parsedData
          .toDF("id", "lon", "lat", "features")
          .withColumn("id",monotonically_increasing_id())
          .as[(Long, Double, Double, Vector)]
      }
      else
      {
        parsedData
      }

    identifiedData
      .map{case(id, lon, lat, features) =>
        (new KeyGeoEntity(LonLatGeoEntity(lon, lat), neighborhoodPartitioningLvl),
          new ClusteringInstance(recordId = id, lonLatLocation = (lon, lat), features = features)
        )
      }
  }
}
