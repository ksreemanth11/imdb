package com.imdb

import com.imdb.source.SourceDataLoading
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object TopMovies {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "/")
    val sparkSession = SparkSession.builder().master("local").appName("IMDB Case Study").getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")
    import sparkSession.implicits._

    val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
    val dataConfig = rootConfig.getConfig("datasets")

    val titleBasicsdata = SourceDataLoading.loadTitleBasicsData(sparkSession, dataConfig)
    println(s"Count of Movies = ${titleBasicsdata.count()}")

    var ratingsDf = SourceDataLoading.loadRatingsData(sparkSession, dataConfig)
    println(s"\nCount of Ratings = ${ratingsDf.count()}")

    val averageNumberOfVotes = math.floor(ratingsDf.select(avg($"numVotes")).collect().map(_(0)).toList(0).toString.toDouble * 100) / 100
    println(s"Avg. no. of votes = $averageNumberOfVotes")

    ratingsDf = ratingsDf
      .withColumn("averageNumberOfVotes", lit(averageNumberOfVotes))
      .withColumn("rank", ($"numVotes"/$"averageNumberOfVotes") * $"averageRating")
      .withColumn("row_num", row_number().over(Window.orderBy(desc("rank"))))
      .filter($"row_num".leq(lit(20)))

    println("\nRetrieve the top 20 movies,")
    val finalDf = ratingsDf.join(titleBasicsdata, Seq("tconst"), "inner")
      .select("tconst", "titleType", "primaryTitle", "genres", "averageRating", "numVotes", "averageNumberOfVotes", "rank")

    finalDf.orderBy(desc("rank")).show(false)
    finalDf.repartition(5).write.mode(SaveMode.Overwrite).parquet(dataConfig.getString("top20.movies"))

    sparkSession.close()
  }
}
