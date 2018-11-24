package com.github.dmarcous.ddbgscan.core.config

import com.github.dmarcous.ddbgscan.core.config.CoreConfig.{DEFAULT_GEO_FILE_DELIMITER, DEFAULT_LATITUDE_POSITION_FIELD_NUMBER, DEFAULT_LONGITUDE_POSITION_FIELD_NUMBER, NO_UNIQUE_ID_FIELD}

case class IOConfig(
   inputPath : String,
   outputFolderPath : String,
   positionId: Int = NO_UNIQUE_ID_FIELD,
   positionLon: Int = DEFAULT_LONGITUDE_POSITION_FIELD_NUMBER,
   positionLat: Int = DEFAULT_LATITUDE_POSITION_FIELD_NUMBER,
   inputDelimiter : String = DEFAULT_GEO_FILE_DELIMITER
)
{

}
