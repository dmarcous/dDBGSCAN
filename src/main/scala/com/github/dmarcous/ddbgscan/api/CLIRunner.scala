package com.github.dmarcous.ddbgscan.api

import com.github.dmarcous.ddbgscan.core.config.CoreConfig._
import com.github.dmarcous.ddbgscan.core.config.{AlgorithmParameters, IOConfig}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.SparkSession

object CLIRunner {

  def main(args: Array[String]) :Unit =
  {
    // Create spark context
    val appName="dDBGSCAN"
    val spark =
      SparkSession
        .builder()
        .appName(appName)
        .getOrCreate()

    val conf = parseArgs(args)

    // Run clustering main flow algorithm
    dDBGSCANRunner.run(spark, conf)
  }

  def parseArgs(args: Array[String]) : RuntimeConfig = {
    val usage = """
    Usage: /usr/lib/spark/bin/spark-submit --class com.dmarcous.github.ddbgscan.api.CLIRunner [filename.jar]
    --inputFilePath [string] --outputFolderPath [string]
    --epsilon [double] --minPts [int]
    [--positionFieldId int] [--positionFieldLon int] [--positionFieldLat int]
    [--inputFieldDelimiter int]
    [--neighborhoodPartitioningLvl int] [--isNeighbourInstances_function_code int]
    [--numPartitions int]
    """

    var inputPath : String = ""
    var outputFolderPath : String = ""
    var epsilon : Double = Double.NaN
    var minPts : Int = Double.NaN.toInt
    var positionFieldId: Int = NO_UNIQUE_ID_FIELD
    var positionFieldLon: Int = DEFAULT_LONGITUDE_POSITION_FIELD_NUMBER
    var positionFieldLat: Int = DEFAULT_LATITUDE_POSITION_FIELD_NUMBER
    var inputFieldDelimiter : String = DEFAULT_GEO_FILE_DELIMITER
    var neighborhoodPartitioningLvl : Int = MISSING_NEIGHBORHOOD_LVL
    var isNeighbourInstances : (Vector, Vector) => Boolean = DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION
    var numPartitions : Int = DEFAULT_NUM_PARTITIONS

    args.sliding(2, 2).toList.collect{
      case Array("--inputFilePath", argInputFilePath: String) => inputPath = argInputFilePath
      case Array("--positionFieldId", argPositionFieldId: String) => positionFieldId = argPositionFieldId.toInt
      case Array("--positionFieldLon", argPositionFieldLon: String) => positionFieldLon= argPositionFieldLon.toInt
      case Array("--positionFieldLat", argPositionFieldLat: String) => positionFieldLat = argPositionFieldLat.toInt
      case Array("--inputFieldDelimiter", argOnputFieldDelimiter: String) => inputFieldDelimiter = argOnputFieldDelimiter
      case Array("--outputFolderPath", argOutputFolderPath: String) => outputFolderPath = argOutputFolderPath
      case Array("--epsilon", argEpsilon: String) => epsilon = argEpsilon.toDouble
      case Array("--minPts", argMinPts: String) => minPts = argMinPts.toInt
      case Array("--neighborhoodPartitioningLvl", argNeighborhoodPartitioningLvl: String) => neighborhoodPartitioningLvl = argNeighborhoodPartitioningLvl.toInt
      case Array("--isNeighbourInstances_function_code", argIsNeighbourInstances_function_code: String) => isNeighbourInstances = NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION_TRANSLATOR(argIsNeighbourInstances_function_code.toInt)
      case Array("--numPartitions", argNumPartitions: String) => numPartitions = argNumPartitions.toInt
    }

    // Make sure all mandatory params are in place
    if (inputPath.isEmpty || outputFolderPath.isEmpty || epsilon.isNaN || minPts.isNaN)
    {
      println(usage)
      System.exit(1)
    }

    println("inputPath : " + inputPath)
    println("positionFieldId : " + positionFieldId)
    println("positionFieldLon : " + positionFieldLon)
    println("positionFieldLat : " + positionFieldLat)
    println("inputFieldDelimiter : " + inputFieldDelimiter)
    println("outputFolderPath : " + outputFolderPath)
    println("epsilon : " + epsilon)
    println("minPts : " + minPts)
    println("neighborhoodPartitioningLvl : " + neighborhoodPartitioningLvl)
    println("isNeighbourInstances function code : " + isNeighbourInstances.toString())
    println("numPartitions : " + numPartitions)

    RuntimeConfig(
      IOConfig(
        inputPath,
        outputFolderPath,
        positionFieldId,
        positionFieldLon,
        positionFieldLat,
        inputFieldDelimiter,
        numPartitions
      ),
      AlgorithmParameters(
        epsilon,
        minPts,
        neighborhoodPartitioningLvl,
        isNeighbourInstances
      )
    )
  }
}
