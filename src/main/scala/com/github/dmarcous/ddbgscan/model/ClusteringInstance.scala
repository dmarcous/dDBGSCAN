package com.github.dmarcous.ddbgscan.model

import com.github.dmarcous.ddbgscan.core.CoreConfig
import com.github.dmarcous.ddbgscan.core.CoreConfig.{ClusteringInstanceStatus, UNKNOWN_CLUSTER}
import org.apache.spark.ml.linalg.Vector

case class ClusteringInstance(
   var cluster : Long = UNKNOWN_CLUSTER,
   var isVisited : Boolean = false,
   var instanceStatus : CoreConfig.ClusteringInstanceStatus.Value = ClusteringInstanceStatus.UNKNOWN,
   features : Vector
) {
}
