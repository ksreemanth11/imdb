package com.imdb

import com.imdb.source.SourceDataLoading
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object TopMovieWatchers {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "/")
    val sparkSession = SparkSession.builder().master("local").appName("IMDB Case Study").getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")
    import sparkSession.implicits._

    val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
    val dataConfig = rootConfig.getConfig("datasets")

    val movWatchersRatingsCountDf = SourceDataLoading.loadMovWatchersRatingsData(sparkSession, dataConfig)

    movWatchersRatingsCountDf.show(20, false)

    val top20MoviesDf = sparkSession.read
      .parquet(dataConfig.getString("top20.movies"))
    top20MoviesDf.show()

    val top20MoviesAndWatchersDf = movWatchersRatingsCountDf.join(top20MoviesDf, Seq("tconst"))
        .join(movWatchersRatingsCountDf, Seq("nconst"))
        .toDF("nconst", "tconst", "numRatings", "titleType", "primaryTitle", "genres", "averageRating", "numVotes", "averageNumberOfVotes", "rank", "tconst", "numRatings2")
        .withColumn("row_num", row_number().over(Window.orderBy(desc("numRatings"))))

    top20MoviesAndWatchersDf
      .filter($"row_num".leq(lit(20)))
      .select("nconst", "numRatings")
      .show()

    sparkSession.close()
  }
}
