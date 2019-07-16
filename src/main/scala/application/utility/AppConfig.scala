package application.utility

import com.typesafe.config.{Config, ConfigFactory}
import scala.util.Properties

class AppConfig(confFile: String) {

  val config = ConfigFactory.load(confFile)

  // Spark Parameters
  val SPARK_APP_NAME = envOrElseConfig("env.spark_config.SPARK_APP_NAME")
  val SPARK_MASTER = envOrElseConfig("env.spark_config.SPARK_MASTER")
  val SPARK_SQL_WH_DIRECTORY = envOrElseConfig("env.spark_config.SPARK_SQL_WH_DIRECTORY")

  // Data parameters
  val DATA_CSV_DELIMITER = envOrElseConfig("env.app_data.DATA_CSV_DELIMITER")
  val DATA_ANIME_FILE = envOrElseConfig("env.app_data.DATA_ANIME_FILE")
  val DATA_RATING_FILE = envOrElseConfig("env.app_data.DATA_RATING_FILE")
  val DATA_OUTPUT_PATH = envOrElseConfig("env.app_data.DATA_OUTPUT_PATH")

  def envOrElseConfig(name: String): String = {
    Properties.envOrElse(
      name.toUpperCase.replaceAll("""\.""", "_"),
      config.getString(name)
    )
  }
}
