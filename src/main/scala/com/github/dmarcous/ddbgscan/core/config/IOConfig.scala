package com.github.dmarcous.ddbgscan.core.config

import com.github.dmarcous.ddbgscan.core.config.CoreConfig.{DEFAULT_GEO_FILE_DELIMITER,
  DEFAULT_LATITUDE_POSITION_FIELD_NUMBER, DEFAULT_LONGITUDE_POSITION_FIELD_NUMBER,
  NO_UNIQUE_ID_FIELD, DEFAULT_NUM_PARTITIONS, DEFAULT_DEBUG}

case class IOConfig(
   inputPath : String,
   outputFolderPath : String,
   positionId: Int = NO_UNIQUE_ID_FIELD, // Should include unique long values
   positionLon: Int = DEFAULT_LONGITUDE_POSITION_FIELD_NUMBER,
   positionLat: Int = DEFAULT_LATITUDE_POSITION_FIELD_NUMBER,
   inputDelimiter : String = DEFAULT_GEO_FILE_DELIMITER,
   numPartitions : Int = DEFAULT_NUM_PARTITIONS,
   debug : Boolean = DEFAULT_DEBUG
)
{

}
