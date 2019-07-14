package application.query

import application.spark.SparkUtils.{createSQLView, saveCSV}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQLQuery {

  def sparkQuery(sparkSession:SparkSession,csvDelimiter:String,topn:String,animeFile:String,ratingFile:String): DataFrame = {

    // Load anime CSV file and create SQL view
    createSQLView(sparkSession,csvDelimiter,animeFile,"v_anime")

    // Load rating CSV file and create SQL view
    createSQLView(sparkSession,csvDelimiter,ratingFile,"v_rating")

    // Filter anime table by TV series with over 10 episodes
    sparkSession.sql(
      "SELECT anime_id, name, genre " +
        "FROM v_anime " +
        "WHERE type = 'TV' " +
        "AND episodes > 10 ")
      .createOrReplaceTempView("v_anime_filter")

    // Calculate rating from rating table
    sparkSession.sql(
      "SELECT a.anime_id, AVG(rating) AS rating, COUNT(*) AS num_rating " +
        "FROM v_rating a " +
        "GROUP BY a.anime_id")
      .createOrReplaceTempView("v_rating_group")

    // Determines genres for the top N most rated
    sparkSession.sql(
      "SELECT a.anime_id, a.name, a.genre, b.rating, b.num_rating " +
        "FROM v_anime_filter a, v_rating_group b " +
        "WHERE a.anime_id = b.anime_id " +
        "ORDER BY b.num_rating DESC " +
        "LIMIT " + topn )
      .createOrReplaceTempView("v_topn")

    // Convert to DF
    val resultDF = sparkSession.table("v_topn").toDF()

    // drop the temp views
    sparkSession.catalog.dropTempView("v_anime")
    sparkSession.catalog.dropTempView("v_rating")
    sparkSession.catalog.dropTempView("v_anime_filter")
    sparkSession.catalog.dropTempView("v_rating_group")
    sparkSession.catalog.dropTempView("v_topn")

    return resultDF
  }

  // execute function where the action happens
  def sparkExecute(sparkSession:SparkSession,csvDelimiter:String,topN:String,animeFile:String,ratingFile:String,outputPath:String) {
    // Determines the top N most rated TV series
    // Show the result
    val resultDF = sparkQuery(sparkSession,csvDelimiter,topN,animeFile,ratingFile)
    resultDF
      .select("anime_id", "genre", "rating", "num_rating")
      .show(false)
    // Save Result
    saveCSV(resultDF,"Y",csvDelimiter,outputPath)
  }
}
