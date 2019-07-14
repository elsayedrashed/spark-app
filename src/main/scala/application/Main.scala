package application

import org.apache.log4j.{Level, Logger}
import application.spark.SparkSessionCreate.createSession
import application.query.SparkSQLQuery
import org.apache.spark.sql.SparkSession

object Main extends App{

  // run function where the action happens
  def run(sparkSession:SparkSession,csvDelimiter:String,topn:String,animeFile:String,ratingFile:String,outputPath:String) {

    println("Determines the top " + topN + " most rated TV series")
    SparkSQLQuery.sparkExecute(spark, csvDelimiter, topN, animeFile, ratingFile, outputPath)
  }

  // Set the log level to only print errors
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Set constants
  val sparkAppName = "spark-app"
  val sparkMaster = "local[4]"
  val sqlWhDirectory = "C:/sqlWhDirectory/"

  // Create SparkSession
  val spark = createSession(sparkAppName, sparkMaster, sqlWhDirectory)

  // Get command line arguments
  val csvDelimiter = "."
  val topN = "10"
  val animeFile = "src/test/resources/input/anime.csv"
  val ratingFile = "src/test/resources/input/rating.csv.gz"
  var outputPath = "src/test/resources/output/top10"

  // Run Jobs
  run(spark,csvDelimiter,topN,animeFile,ratingFile,outputPath)
}
