package application

import org.apache.log4j.{Level, Logger}
import application.spark.SparkSessionCreate.createSession
import application.spark.{SparkDeltaMerger, SparkSessionCreate}
import application.utility.AppConfig


object Main extends App{

  // run function where the action happens
  def run() {

    import spark.implicits._

    val row1 = ("1", "1", "val1", "val2")
    val row2 = ("1", "2", "val3", "val4")
    val row3 = ("2", "1", "val5", "val6")
    val delta1 = Seq(row1, row2, row3).toDF("pk1", "pk2", "col1", "col2")

    val row4 = ("1", "2", "val7", "val8")
    val delta2 = Seq(row4).toDF("pk1", "pk2", "col1", "col2")

    println("Merge Data Sets")
    val result = SparkDeltaMerger.merge(Seq(delta1, delta2), Seq("pk1", "pk2"))
    result.show()
  }

  // Set the log level to only print errors
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Get Parameters
  val ac = new AppConfig("env-dev.conf")

  println(ac.SPARK_APP_NAME)
  println(ac.SPARK_MASTER)
  println(ac.SPARK_SQL_WH_DIRECTORY)

  // Use SparkSession interface
  val spark = SparkSessionCreate.createSession(ac.SPARK_APP_NAME,ac.SPARK_MASTER,ac.SPARK_SQL_WH_DIRECTORY)

  // Run Jobs
  run()
}
