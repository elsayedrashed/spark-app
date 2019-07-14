package application.spark

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row}

object SparkDeltaMerger {

  def merge(datasets: Seq[Dataset[Row]], primaryKey: Seq[String]) = {
    val deltaIndexCol = "DELTA_INDEX"
    val rankCol = "DELTA_RANK"
    val windowSpec = Window
      .partitionBy(primaryKey.map(str => col(str)): _*)
      .orderBy(col(deltaIndexCol).desc)

    datasets
      .zipWithIndex
      .map {
        case (dataset, index) => dataset.withColumn(deltaIndexCol, lit(index))
      }
      .reduceLeft { (ds1, ds2) => ds1.union(ds2) }
      .withColumn(rankCol, dense_rank().over(windowSpec))
      .filter(col(rankCol).===(1))
      .drop(deltaIndexCol, rankCol)
  }

}
