name := "dDBGSCAN"

version := "1.0.0"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.3"

resolvers ++= Seq(
  "All Spark Repository -> bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"
)

// Scala version override
dependencyOverrides += "org.scala-lang" % "scala-compiler" % scalaVersion.value

// Spark
libraryDependencies ++= Seq(
  "org.apache.spark"      %% "spark-core"       % sparkVersion % "provided",
  "org.apache.spark"      %% "spark-sql"        % sparkVersion % "provided",
  "org.apache.spark"      %% "spark-mllib"      % sparkVersion % "provided",
  "org.apache.spark"      %% "spark-graphx"     % sparkVersion % "provided"
)
// Spark GraphFrames
libraryDependencies += "graphframes" % "graphframes" % "0.7.0-spark2.4-s_2.11"

// S2 utils
libraryDependencies += "com.github.dmarcous" % "s2utils_2.11" % "1.1.1"

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

// Assembly for fat jar (EMR ready) settings
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyJarName in assembly := s"${name.value}_${scalaBinaryVersion.value}-${sparkVersion}_${version.value}.jar"
