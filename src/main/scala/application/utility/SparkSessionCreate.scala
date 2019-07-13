package application.utility

import org.apache.spark.sql.SparkSession

object SparkSessionCreate {
  def createSession(sparkAppName:String,sparkMaster:String,sqlWhDirectory:String): SparkSession = {
    val spark = SparkSession
      .builder
      .appName(sparkAppName)
      .master(sparkMaster)
      .config("spark.sql.warehouse.dir", sqlWhDirectory)
      .getOrCreate()

    return spark
  }
}
