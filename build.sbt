name := "spark-app"

organization := "spark-app"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.4.3"
val ScalaTestVersion = "3.0.8"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

// Apache Spark
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided" withSources(),
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided" withSources()
)

// Test Framework
logBuffered in Test := false
libraryDependencies ++= Seq(
  "org.scalactic" %% "scalactic" % ScalaTestVersion,
  "org.scalatest" %% "scalatest" % ScalaTestVersion % "test"
)
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest,
  "-y", "org.scalatest.FunSuite",
  "-y", "org.scalatest.FunSpec",
  "-y", "org.scalatest.PropSpec",
  "-y", "org.scalatest.FlatSpec",
  "-y", "org.scalatest.FeatureSpec"
)
