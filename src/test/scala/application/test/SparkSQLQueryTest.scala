package application.test

import application.spark.SparkSessionCreate
import application.query.SparkSQLQuery
import org.apache.log4j.{Level, Logger}
import org.scalatest.FunSuite

class SparkSQLQueryTest extends FunSuite {

  // Set the log level to only print errors
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Set constants
  val appName = "spark-app"
  val sparkMaster = "local[4]"
  val sqlWhDirectory = "C:/sqlWhDirectory/"

  // Use SparkSession interface
  val spark = SparkSessionCreate.createSession(appName,sparkMaster,sqlWhDirectory)

  test("Test Case 1: Top 10") {

    val csvDelimiter = ","
    val topN = "10"
    val animeFile = "src/test/resources/input/anime.csv"
    val ratingFile = "src/test/resources/input/rating.csv.gz"
    val outputPath = "src/test/resources/output/top10"

    println("Determines the top " + topN + " most rated TV series")
    SparkSQLQuery.sparkExecute(spark,csvDelimiter,topN,animeFile,ratingFile,outputPath)
    assert(1 === 1)
  }

  test("Test Case 2: Top 20") {

    val csvDelimiter = ","
    val topN = "20"
    val animeFile = "src/test/resources/input/anime.csv"
    val ratingFile =
      "src/test/resources/input/rating.csv.gz"
    val outputPath = "src/test/resources/output/top20"

    println("Determines the top " + topN + " most rated TV series")
    SparkSQLQuery.sparkExecute(spark,csvDelimiter,topN,animeFile,ratingFile,outputPath)
    assert(1 === 1)
  }
}
