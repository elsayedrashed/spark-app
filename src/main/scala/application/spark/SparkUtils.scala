package application.spark

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkUtils {

  def loadCSV(sparkSession:SparkSession,csvDelimiter:String,filename:String): DataFrame = {

    var dataDF = sparkSession.read
      .format("com.databricks.spark.csv")
      .option("header", "true") //reading the headers
      .option("delimiter",csvDelimiter)
      .option("quote","\"")
      .option("mode", "DROPMALFORMED")
      .load(filename)

    return dataDF
  }

  def saveCSV(dataDF:DataFrame,singleFile:String,csvDelimiter:String,outputPath:String) {

    if (singleFile.equalsIgnoreCase("S")) {
      dataDF
        .coalesce(1)
        .write
        .mode("Overwrite")
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter",csvDelimiter)
        .option("quote","\"")
        .option("quoteAll","true")
        .save(outputPath)
    } else {
      dataDF
        .write
        .mode("Overwrite")
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter",csvDelimiter)
        .option("quote","\"")
        .option("quoteAll","true")
        .save(outputPath)
    }
  }

  def createSQLView(sparkSession:SparkSession,csvDelimiter:String,filename:String,viewname:String) {

    // Load CSV file
    loadCSV(sparkSession,csvDelimiter,filename)
      // Creates a temporary view using DataFrame
      .createOrReplaceTempView(viewname)
  }
}
