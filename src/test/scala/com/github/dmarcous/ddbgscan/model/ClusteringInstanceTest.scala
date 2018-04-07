package com.github.dmarcous.ddbgscan.model

import com.github.dmarcous.ddbgscan.core.CoreConfig.{ClusteringInstanceStatus, UNKNOWN_CLUSTER}
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class ClusteringInstanceTest extends FlatSpec{
  val cluster = 1L
  val isVisited = true
  val instanceStatus = ClusteringInstanceStatus.CORE
  val features = Vectors.dense(1.0,2.0,3.0)
  val emptyFeatures = Vectors.zeros(0)

  "Full constructor" should "return a valid object" in
  {
    val instance =
      ClusteringInstance(
        cluster,
        isVisited,
        instanceStatus,
        features
      )

    instance.cluster should equal(cluster)
    instance.isVisited should equal(isVisited)
    instance.instanceStatus should equal(instanceStatus)
    instance.features should equal(features)

  }

  "Short constructor" should "return a valid object with defaults" in
  {
    val instance =
      ClusteringInstance(
        features = emptyFeatures
      )

    instance.cluster should equal(UNKNOWN_CLUSTER)
    instance.isVisited should equal(false)
    instance.instanceStatus should equal(ClusteringInstanceStatus.UNKNOWN)
    instance.features should equal(emptyFeatures)
  }

}
