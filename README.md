# dDBGSCAN

An implementation of distributed density based geospatial clustering of applications with noise 
over large datasets using Apache Spark.

## Details

This project contains an implementation of the dDBGSCAN algorithm including running examples.
Further details in paper : TBD

### Algorithm Steps

  1. Partitioning using S2cell units
  2. Local clustering using DBSCAN with r*-tree
  3. Global merging using graph label propagation

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

Spark 2.2+.

Scala 2.11.

## Usage

### Scala API

```scala
// import 
import com.github.dmarcous.ddbgscan.core.dDBGSCAN

// Run clustering
println("Starting clustering...")
val results = 
	dDBGSCAN.run(spark, data, epsilon, minPts)	        
	    
// Print output
println("results : ")
results.take(100).foreach(println)

```

### Running dDBGSCAN from command line

You can run dDBGSCAN directly form command line using spark-submit.

Parameters stated above

```bash
## Usage : /usr/lib/spark/bin/spark-submit --class com.dmarcous.github.ddbgscan.api.CLIRunner filename.jar inputFilePath outputFolderPath Epsilon MinPts [NeighborhoodPartitioningLvl] [isNeighbourInstances_function_code]

/usr/lib/spark/bin/spark-submit --class com.dmarcous.github.dDBGSCAN.CLIRunner /tmp/dDBGSCAN.jar /tmp/input.txt /tmp/output/ 100 20 14 0
```

## Credits

Written and maintained by :

Daniel Marcous <dmarcous@gmail.com>


