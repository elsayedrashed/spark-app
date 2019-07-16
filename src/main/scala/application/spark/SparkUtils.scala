package application.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, rank}
import org.apache.spark.sql.{Column, Dataset, Row}

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

  def keepFirstLineOfPartition(dataset: Dataset[Row], partitionByColumnsAsString: List[String], orderByColumn: String): Dataset[Row] = {
    val partitionByColumns = partitionByColumnsAsString.map(name => col(name))
    keepFirstLineOfPartition(dataset, partitionByColumns, Seq(desc(orderByColumn)))
  }

  def keepFirstLineOfPartition(dataset: Dataset[Row], partitionByColumns: Seq[Column], orderByColumns: Seq[Column]): Dataset[Row] = {
    val rankColumnName = "RANK"
    rankOverPartition(dataset, rankColumnName, partitionByColumns, orderByColumns)
      .where(col(rankColumnName) === "1")
      .drop(rankColumnName)
  }

  def rankOverPartition(dataset: Dataset[Row], rankColumnName: String, partitionByColumns: Seq[Column], orderByColumns: Seq[Column]) = {
    dataset
      .withColumn(rankColumnName, rank() over Window
        .partitionBy(
          partitionByColumns: _*
        )
        .orderBy(
          orderByColumns: _*
        )
      )
  }
}
