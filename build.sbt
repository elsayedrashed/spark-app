name := "spark-app"

organization := "spark-app"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.4.3"
val ScalaTestVersion = "3.0.8"
val typeSafeVersion = "1.3.4"
val guavaVersion = "23.0"
val specs2Version = "4.6.0"
val slf4jVersion = "1.7.26"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

// Apache Spark
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided" withSources(),
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided" withSources(),
  "com.typesafe" % "config" % typeSafeVersion,
  "org.slf4j" % "slf4j-api" % slf4jVersion,
  "org.slf4j" % "slf4j-simple" % slf4jVersion % Test,
  "org.specs2" %% "specs2-core" % specs2Version % Test,
  "org.specs2" %% "specs2-mock" % specs2Version % Test,
  "com.google.guava" % "guava" % guavaVersion
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
