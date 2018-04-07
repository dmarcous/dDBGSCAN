package com.github.dmarcous.ddbgscan.api

import com.github.dmarcous.ddbgscan.core.CoreConfig.{DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION, MISSING_NEIGHBORHOOD_LVL, NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION_TRANSLATOR}
import com.github.dmarcous.ddbgscan.core.{AlgorithmParameters, GeoPropertiesExtractor, dDBGSCAN}
import org.apache.spark.sql.SparkSession

object CLIRunner {

  def main(args: Array[String])
  {
    // Create spark context
    val appName="dDBGSCAN"
    val spark =
      SparkSession
        .builder()
        .appName(appName)
        .getOrCreate()

    val conf = parseArgs(args)

    // Read input file
    val data =
      spark.read
        .textFile(conf.inputPath)

    // Extract geo data from input and keep rest
    val clusteringData =
      GeoPropertiesExtractor.fromLonLatDelimitedFile(
        spark,
        data,
        positionLon = 0, positionLat = 1)

    // Run GloVe
    println("Starting clustering...")
    val results =
      dDBGSCAN.run(spark, clusteringData,
        conf.parameters)

    // Write output
    println("Writing results...")
    results.write
      .csv(conf.outputFolderPath)
  }

  private def parseArgs(args: Array[String]) : AlgorithmConfig = {
    val usage = """
    Usage: /usr/lib/spark/bin/spark-submit --class com.dmarcous.github.ddbgscan.api.CLIRunner [filename.jar] [inputFilePath] [outputFolderPath] [Epsilon] [MinPts] [NeighborhoodPartitioningLvl] [isNeighbourInstances_function_code]
    """
    // Input params
    if (args.length < 4 || args.length > 6)
    {
      println(usage)
      System.exit(1)
    }

    val inputPath = args(0)
    val outputFolderPath = args(1)
    val epsilon = args(2).toDouble
    val minPts = args(3).toInt
    val neighborhoodPartitioningLvl = if (args.length > 4) args(4).toInt else MISSING_NEIGHBORHOOD_LVL
    val isNeighbourInstances =
      if (args.length > 5 &&
          args(5).toInt < NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION_TRANSLATOR.keySet.size)
        NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION_TRANSLATOR.getOrElse(args(5).toInt, DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION)
      else DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION

    println("inputPath : " + inputPath)
    println("outputFolderPath : " + outputFolderPath)
    println("epsilon : " + epsilon)
    println("minPts : " + minPts)
    println("neighborhoodPartitioningLvl : " + neighborhoodPartitioningLvl)
    println("isNeighbourInstances : " + isNeighbourInstances.toString)

    AlgorithmConfig(
      inputPath,
      outputFolderPath,
      AlgorithmParameters(
        epsilon,
        minPts,
        neighborhoodPartitioningLvl,
        isNeighbourInstances
      )
    )
  }
}
