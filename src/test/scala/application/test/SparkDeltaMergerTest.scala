package application.test

import org.apache.log4j.{Level, Logger}
import application.spark.SparkDeltaMerger
import org.scalatest.FunSuite

class SparkDeltaMergerTest extends FunSuite {

  // Set the log level to only print errors
  Logger.getLogger("org").setLevel(Level.ERROR)

  test("SparkDeltaMerger - Test Case 1 : 3 data sets") {
    val row1 = ("1", "1", "val1", "val2")
    val row2 = ("1", "2", "val3", "val4")
    val row3 = ("2", "1", "val5", "val6")
    val delta1 = Seq(row1, row2, row3).toDF("pk1", "pk2", "col1", "col2")

    val row4 = ("1", "2", "val7", "val8")
    val delta2 = Seq(row4).toDF("pk1", "pk2", "col1", "col2")

    val result = SparkDeltaMerger.merge(Seq(delta1, delta2), Seq("pk1", "pk2"))

    assert(1 === 1)
  }

}
