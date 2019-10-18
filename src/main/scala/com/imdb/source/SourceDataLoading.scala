package com.imdb.source

import com.typesafe.config.Config
import org.apache.spark.sql.functions.{explode, size, split}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

object SourceDataLoading {
  def loadTitleBasicsData(sparkSession: SparkSession, dataConfig: Config): DataFrame = {
    println("\n'Title Basics' data analysis,\n")
    val titleBasicsDfSchema = StructType(
      Seq(StructField("tconst", StringType, false),
        StructField("titleType", StringType, false),
        StructField("primaryTitle", StringType, false),
        StructField("originalTitle", StringType, false),
        StructField("isAdult", IntegerType, false),
        StructField("startYear", IntegerType, false),
        StructField("endYear", IntegerType, false),
        StructField("runtimeMinutes", IntegerType, false),
        StructField("genres", StringType, true)
      )
    )

    val titleBasicsDf = sparkSession.read
      .option("header", "true")
      .option("delimiter", "\\t")
      .option("nullValue", "\\N")
      .format("csv")
      .schema(titleBasicsDfSchema)
      .load(dataConfig.getString("title.basics"))

    titleBasicsDf.printSchema()
    titleBasicsDf.show(10, false)

    import sparkSession.implicits._
    val titleBasicsTotalCount = titleBasicsDf.count()
    println(s"Total no. of records in Titles' Dataset = ${titleBasicsTotalCount}")
    println(s"\nUnique Titles : ${titleBasicsDf.select("titleType").distinct().collect().map(_(0)).toList}")

    println("\nKeeping only 'tvMovie' and 'movie' titles,")
    val filteredTitleBasicsDf = titleBasicsDf.filter($"titleType".isin("movie", "tvMovie"))
    val filteredTitleBasicsCount = filteredTitleBasicsDf.count()
    val percOfTitleBasicsData = math.floor((filteredTitleBasicsCount.toDouble/titleBasicsTotalCount.toDouble) * math.pow(10,4)) / 100
    println(s"\nTotal no. of Movie Titles = ${filteredTitleBasicsCount} (${percOfTitleBasicsData}%)")

    return filteredTitleBasicsDf
  }

  def loadRatingsData(sparkSession: SparkSession, dataConfig: Config): DataFrame = {
    println("\n'Ratings' data analysis,")

    val ratingDfSchema = StructType(
      Seq(StructField("tconst", StringType, false),
        StructField("averageRating", DoubleType, false),
        StructField("numVotes", LongType, false)
      )
    )

    import sparkSession.implicits._
    val ratingDf = sparkSession.read
      .option("header", "true")
      .option("delimiter", "\\t")
      .format("csv")
      .schema(ratingDfSchema)
      .load(dataConfig.getString("title.ratings"))
      .repartition(5, $"tconst")

    ratingDf.printSchema()
    ratingDf.show(5, false)

    val ratingsDataTotalCount = ratingDf.count()
    val filteredRatingsDf = ratingDf.filter($"numVotes".geq(50))
    val filteredRatingsDataCount = filteredRatingsDf.count()
    val percOfRatingsData = math.floor((filteredRatingsDataCount.toDouble/ratingsDataTotalCount.toDouble) * math.pow(10,4)) / 100
    println(s"Total no. of records in Ratings Dataset = ${ratingsDataTotalCount}")
    println("\nKeeping only 'movies' with more than equal to 50 ratings,")
    println(s"\nTotal no. of records with minimum 50 votes = ${filteredRatingsDataCount} (${percOfRatingsData}%)")

    return filteredRatingsDf
  }

  def loadMovWatchersRatingsData(sparkSession: SparkSession, dataConfig: Config): DataFrame = {

    import sparkSession.implicits._
    val moviesWatchersDf = sparkSession.read
      .option("header", "true")
      .option("delimiter", "\\t")
      .csv(dataConfig.getString("name.basics"))
      .select("nconst", "knownForTitles")
    //    moviesWatchersDf.show(5, false)

    moviesWatchersDf
      .withColumn("knownForTitles", split($"knownForTitles", ","))
      .withColumn("numRatings", size($"knownForTitles"))
      .withColumn("tconst", explode($"knownForTitles"))
      .select("nconst", "tconst", "numRatings")
      .distinct()

  }

}
