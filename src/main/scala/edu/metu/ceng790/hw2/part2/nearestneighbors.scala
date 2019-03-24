package edu.metu.ceng790.hw2.part2

import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.sql.SparkSession

object nearestneighbors {
  def main(args: Array[String]): Unit = {
    val dataSetHomeDir = "hw2_dataset/ml-20m/"

    var spark: SparkSession = null
    spark = SparkSession.builder().appName("Assignment-2_Part-2:Content Based Nearest Neighbors").config("spark.master", "local[*]").getOrCreate()

    //Create Spark Context
    val sc = spark.sparkContext
    //Set log level
    sc.setLogLevel("ERROR")
    sc.setCheckpointDir("/tmp")

    //LOAD DATA
    val dataRaw = sc.textFile(dataSetHomeDir + "ratings.csv")
    val header = dataRaw.first()
    val dataWithNoHeader = dataRaw.filter(e => e.equals(header) == false)

    val dataRawMovies = sc.textFile(dataSetHomeDir + "movies.csv")
    val headerMovies = dataRawMovies.first()
    val dataWithNoHeaderMovies = dataRawMovies.filter(e => e.equals(headerMovies) == false)

    val ratings = dataWithNoHeader.map(line => line.split(',') match {
      case Array(user, movieID, rating, date)
      => Rating(user.toInt, movieID.toInt, rating.toDouble)
    })

    val ratingsGroupByUser = ratings.groupBy(e => e.user).map(x =>
      (x._1, x._2.map(y => y.rating).seq))

    val userAverageRatingMap = ratingsGroupByUser.map(e => {
      (e._1, e._2.sum/e._2.size)
    }).collect().toMap

    println(userAverageRatingMap.size)

    val goodRatings = ratings.filter(e => e.rating >= userAverageRatingMap.get(e.user).get)

    //Built movies: Map[Int, String]
    val moviesRaw = dataWithNoHeaderMovies.map(line => line.split(','))
    val movieNames: Map[Int, String] = moviesRaw.map(e => (e(0).toInt, e(1))).collect().toMap

    //Build Movie Genres
    val movieGenres = dataWithNoHeaderMovies.map(line => line.split(',')).map(e => {
      (e(0).toInt, e(2).split("\\|"))
    }).collect().toMap


  }

}
