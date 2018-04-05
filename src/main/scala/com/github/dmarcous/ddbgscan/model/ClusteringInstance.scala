package com.github.dmarcous.ddbgscan.model

import com.github.dmarcous.ddbgscan.core.CoreConfig
import org.apache.spark.ml.linalg.Vector
import com.github.dmarcous.ddbgscan.core.CoreConfig.UNKNOWN_CLUSTER
import com.github.dmarcous.ddbgscan.core.CoreConfig.ClusteringInstanceStatus

case class ClusteringInstance(
   var cluster : Long = UNKNOWN_CLUSTER,
   var isVisited : Boolean = false,
   var instanceStatus : CoreConfig.ClusteringInstanceStatus.Value = ClusteringInstanceStatus.UNKNOWN,
   features : Vector
) {

  def this(features: Vector) = {
    this(features = features)
  }
}
