package com.github.dmarcous.ddbgscan.model

import com.github.dmarcous.ddbgscan.core.config.CoreConfig.{ClusteringInstanceStatusValue, UNKNOWN_CLUSTER}
import org.apache.spark.ml.linalg.Vector

case class ClusteringInstance(
   var recordId : Long,
   var cluster : Long = UNKNOWN_CLUSTER,
   var isVisited : Boolean = false,
   var instanceStatus : Int = ClusteringInstanceStatusValue.UNKNOWN.value,
   lonLatLocation : (Double, Double),
   features : Vector
) {
}
