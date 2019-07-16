package application.test

import application.spark.SparkSessionCreate
import application.query.SparkSQLQuery
import application.utility.AppConfig
import org.apache.log4j.{Level, Logger}
import org.scalatest.FunSuite

class SparkSQLQueryTest extends FunSuite {

  // Set the log level to only print errors
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Get Parameters
  val ac = new AppConfig("env-qas.conf")

  println(ac.SPARK_APP_NAME)
  println(ac.SPARK_MASTER)
  println(ac.SPARK_SQL_WH_DIRECTORY)

  // Use SparkSession interface
  val spark = SparkSessionCreate.createSession(ac.SPARK_APP_NAME,ac.SPARK_MASTER,ac.SPARK_SQL_WH_DIRECTORY)

  test("Test Case 1: Top 10") {
    val topN = "10"
    println("Determines the top " + topN + " most rated TV series")
    SparkSQLQuery.sparkExecute(spark,ac.DATA_CSV_DELIMITER,topN,ac.DATA_ANIME_FILE,ac.DATA_RATING_FILE,ac.DATA_OUTPUT_PATH)
    assert(1 === 1)
  }

  test("Test Case 2: Top 20") {
    val topN = "20"
    println("Determines the top " + topN + " most rated TV series")
    SparkSQLQuery.sparkExecute(spark,ac.DATA_CSV_DELIMITER,topN,ac.DATA_ANIME_FILE,ac.DATA_RATING_FILE,ac.DATA_OUTPUT_PATH)
    assert(1 === 1)
  }
}
