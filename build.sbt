name := "imdb-usecase"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  // Spark Core and Spark SQL dependencies
  "org.apache.spark" % "spark-core_2.11" % "2.1.1",
  "org.apache.spark" % "spark-sql_2.11" % "2.1.1",

  //configuration file dependency
  "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
  "com.typesafe" % "config" % "1.2.1"
)


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}