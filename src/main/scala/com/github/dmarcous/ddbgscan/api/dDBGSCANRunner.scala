package com.github.dmarcous.ddbgscan.api

import com.github.dmarcous.ddbgscan.core.algo.dDBGSCAN
import com.github.dmarcous.ddbgscan.core.preprocessing.GeoPropertiesExtractor
import org.apache.spark.sql.SparkSession

object dDBGSCANRunner {

  def run(@transient spark: SparkSession, conf: RuntimeConfig) :Unit =
  {
    // Setup - make sure to have spark checkpoint dir set for graphframes connected components algorithm
    spark.sparkContext.setCheckpointDir("/tmp")

    // Read input file
    val data =
      spark.read
        .textFile(conf.ioConfig.inputPath)

    // Extract geo data from input and keep rest
    println("Preprocessing...")
    dDBGSCAN.setJobStageNameInSparkUI(spark, "Preprocessing",
      "Stage 0 - Create clustering instances dataset keyed by geo")
    val clusteringData =
      GeoPropertiesExtractor.fromLonLatDelimitedFile(
        spark,
        data,
        conf.parameters.neighborhoodPartitioningLvl,
        conf.ioConfig)

    // Run clustering algorithm
    println("Starting clustering...")
    val results =
      dDBGSCAN.run(spark, clusteringData,
        conf.parameters)

    // Write output
    println("Writing results...")
    dDBGSCAN.setJobStageNameInSparkUI(spark, "Output",
      "Stage 4 - Writing output as CSV")
    results.write
      .csv(conf.ioConfig.outputFolderPath)
  }
}
