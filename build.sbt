name := "dDBGSCAN"

version := "1.0.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.2"

resolvers ++= Seq(
  "All Spark Repository -> bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"
)

// Spark
libraryDependencies ++= Seq(
  "org.apache.spark"      %% "spark-core"       % sparkVersion,
  "org.apache.spark"      %% "spark-sql"        % sparkVersion,
  "org.apache.spark"      %% "spark-mllib"      % sparkVersion,
  "org.apache.spark"      %% "spark-graphx"     % sparkVersion
)
// Spark GraphFrames
libraryDependencies += "graphframes" % "graphframes" % "0.6.0-spark2.3-s_2.11"

// S2 utils
libraryDependencies += "com.github.dmarcous" %% "s2utils" % "1.1.1"

// R*tree
libraryDependencies += "com.github.davidmoten" % "rtree" % "0.8.4"
libraryDependencies += "com.github.davidmoten" % "grumpy-core" % "0.2.2"
libraryDependencies += "io.reactivex" % "rxscala_2.11" % "0.26.5"

// Testing framework
libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.+" % "test"

// POM settings for Sonatype
organization := "com.github.dmarcous"
homepage := Some(url("https://github.com/dmarcous/dDBGSCAN"))
scmInfo := Some(ScmInfo(url("https://github.com/dmarcous/dDBGSCAN"),
  "scm:git@github.com:dmarcous/dDBGSCAN.git"))
developers := List(Developer("dmarcous",
  "Daniel Marcous",
  "dmarcous@gmail.com",
  url("https://github.com/dmarcous")))
licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))
