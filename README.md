# dDBGSCAN

An implementation of distributed density based geospatial clustering of applications with noise 
over large datasets using Apache Spark.

## Details

This project contains an implementation of the dDBGSCAN algorithm including running examples.
Further details in paper : TBD

### Algorithm Steps

  1. Partitioning using S2cell units
  2. Local clustering using DBSCAN with r*-tree
  3. Global merging using graph(frames) connected components
s
### Parameters

  1. Epsilon (ε) - maximal meters distance between points to be defined as “density
     reachable” and assigned to the same cluster
  2. MinPts - minimum number of points in a cluster (clusters with size lower than MinPts will
     be classified as noise).
  3. NeighborhoodPartitioningLvl - S2 Cell level for data partitioning. Defaults to level where
     the smallest cell area >= 16ε .
  4. isNeighbourInstances - expandable filtering function for enabling extending the
     clustering phase into multiple dimensions. Defaults to True.
     Input using an integer code of a function given the translator : com.github.dmarcous.ddbgscan.core.CoreConfig.NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION_TRANSLATOR 

### Caveats

  1. Current version supports only numeric data as vector values (for distances compute between clustering instances)
  2. Current version does not support null values in data vectors

## Requirements

Spark 2.3.+

Scala 2.11.+

## Usage

### Scala API

```scala
import com.github.dmarcous.ddbgscan.api.RuntimeConfig
import com.github.dmarcous.ddbgscan.api.dDBGSCANRunner
import com.github.dmarcous.ddbgscan.core.config.CoreConfig._
import com.github.dmarcous.ddbgscan.core.config.{AlgorithmParameters, IOConfig}
import java.io.File
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

// Spark settings
val spark =
  SparkSession
    .builder()
    // .master("local") - set this only for testing on your local machine
    .appName("dDBGSCANRunner")
    .getOrCreate()

// Run conf
val epsilon = 100.0
val minPts = 3
val neighborhoodPartitioningLvl = 15
val isNeighbourInstances = DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION
val parameters = AlgorithmParameters(
epsilon,
minPts,
neighborhoodPartitioningLvl,
isNeighbourInstances
)
val inputPath = "./src/test/resources/complexLonLatDelimitedGeoData.csv"
val outputFolderPath = "/tmp/dDBGSCAN/"
val positionId = 0
val positionLon = DEFAULT_LONGITUDE_POSITION_FIELD_NUMBER+1
val positionLat = DEFAULT_LATITUDE_POSITION_FIELD_NUMBER+1
val delimiter = DEFAULT_GEO_FILE_DELIMITER
val ioConfig = IOConfig(
inputPath,
outputFolderPath,
positionId,
positionLon,
positionLat,
delimiter
)
val conf =
RuntimeConfig(
  ioConfig,
  parameters
)

// Clean output before run if necessary
FileUtils.deleteQuietly(new File(outputFolderPath))

// Run algorithm (preprocess, cluster, write results)
dDBGSCANRunner.run(spark, conf)

```

### Scala Advanced Usage API

```scala
import com.github.dmarcous.ddbgscan.core.config.CoreConfig.ClusteringInstanceStatusValue.{BORDER, CORE, NOISE}
import com.github.dmarcous.ddbgscan.core.config.CoreConfig._
import com.github.dmarcous.ddbgscan.core.config.{AlgorithmParameters, IOConfig}
import com.github.dmarcous.ddbgscan.core.preprocessing.GeoPropertiesExtractor
import com.github.dmarcous.ddbgscan.core.algo.dDBGSCAN
import org.apache.spark.sql.SparkSession

// Spark setup
val spark =
SparkSession
  .builder()
  // .master("local") - set this only for testing on your local machine
  .appName("dDBGSCANAdvancedUsage")
  .getOrCreate()
import spark.implicits._
// Important for merging phase (connected components) to work
spark.sparkContext.setCheckpointDir("/tmp")

// Preload sample data to cluster ** REPLACE WITH LOADING YOUR DATA ***
val complexLonLatDelimitedGeoData =
spark.createDataset(
  Seq(
    // Pair cluster
    "11,34.778023,32.073889,6,7", //Address - Tsemach Garden
    "12,34.778137,32.074229,10,11", //Address - Dizzy21-25
    "7,34.778982,32.074499,13,14", //Address - Shmaryahu Levin St 3
    "8,34.778998,32.075054,15,16", //Address - Shmaryahu Levin St 9,
    "9,34.779212,32.075217,17,18", //Address - Shmaryahu Levin St 14,
    "10,34.779861,32.075049,19,20", //Address - Sderot Chen 11,
    "1,34.779046,32.073903,21,22", //Adress - Tarsat
    "2,34.779765,32.073458,23,24", //Adress - HabimaSquare
    // NOISE
    "3,34.775628,32.074032,5,5", //Address - Voodoo
    //Triplet cluster
    "4,34.777112,32.0718015,8,9", //Address - Haimvelish1-7
    "5,34.777547,32.072729,11,2", //Address - BenTsiyon22-28
    "6,34.777558,32.072565,3,4" //Address - Warburg9-3
  ))

// IO parameters - cloud/local works
val inputPath = "s3://my.bucket/ddbgscan/inputs/input.csv"
val outputFolderPath = "s3://my.bucket/ddbgscan/outputs/"
val positionId = 0
val positionLon = DEFAULT_LONGITUDE_POSITION_FIELD_NUMBER+1
val positionLat = DEFAULT_LATITUDE_POSITION_FIELD_NUMBER+1
val delimiter = DEFAULT_GEO_FILE_DELIMITER
val ioConfig = IOConfig(
inputPath,
outputFolderPath,
positionId,
positionLon,
positionLat,
delimiter
)

// Algorithm parameters
val S2_LVL = 15
val epsilon= 100.0
val minPts = 3
val params =
AlgorithmParameters(
  epsilon,
  minPts,
  S2_LVL,
  DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION
)

// Pre-processing utilities to prepare data for clustering 
val clusteringDataset =
GeoPropertiesExtractor.fromLonLatDelimitedFile(
  spark, complexLonLatDelimitedGeoData, S2_LVL, ioConfig
)

// Run clustering algorithm
val globallyClusteredData =
  dDBGSCAN.run(spark, clusteringDataset, params)

// Possible do something with results like writing to CSV
globallyClusteredData.write.csv(outputFolderPath)

```

### Running dDBGSCAN from command line

You can run dDBGSCAN directly form command line using spark-submit.

Parameters stated above

```bash
## Usage : 
/usr/lib/spark/bin/spark-submit --class com.dmarcous.github.ddbgscan.api.CLIRunner [filename.jar]
--inputFilePath [string] --outputFolderPath [string]
--epsilon [double] --minPts [int]
[--positionFieldId int] [--positionFieldLon int] [--positionFieldLat int]
[--inputFieldDelimiter int]
[--neighborhoodPartitioningLvl int] [--isNeighbourInstances_function_code int]

## Example 1 - short form
/usr/lib/spark/bin/spark-submit --class com.dmarcous.github.dDBGSCAN.CLIRunner /tmp/dDBGSCAN.jar
/tmp/input.txt /tmp/output/ 100 20 14 0

## Example 2 - long form
/usr/lib/spark/bin/spark-submit --class com.dmarcous.github.dDBGSCAN.CLIRunner /tmp/dDBGSCAN.jar 
--inputFilePath ./src/test/resources/complexLonLatDelimitedGeoData.csv
--outputFolderPath /tmp/dDBGSCAN/
--positionFieldId 0 --positionFieldLon 1 --positionFieldLat 2 --inputFieldDelimiter ,
--epsilon 100.0 --minPts 3 --neighborhoodPartitioningLvl 15
```

## Credits

Written and maintained by :

Daniel Marcous <dmarcous@gmail.com>


