package edu.metu.ceng790.hw2.part2

import javafx.animation.Animation
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object nearestneighbors {
  def main(args: Array[String]): Unit = {
    val dataSetHomeDir = "hw2_dataset/ml-20m/"

    var spark: SparkSession = null
    spark = SparkSession.builder()
      .appName("Assignment-2_Part-2:Content Based Nearest Neighbors")
      .config("spark.master", "local[*]")
      .getOrCreate()

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

    val movieGenresData = List(
      "Action",
      "Adventure",
      "Animation",
      "Children's",
      "Comedy",
      "Crime",
      "Documentary",
      "Drama",
      "Fantasy",
      "Film-Noir",
      "Horror",
      "Musical",
      "Mystery",
      "Romance",
      "Sci-Fi",
      "Thriller",
      "War",
      "Western"
    )

    val testUser = Map(
      "Action" -> 6,
      "Adventure" -> 4,
      "Animation" -> 34,
      "Children's" -> 1,
      "Comedy" -> 23,
      "Crime" -> 1,
      "Documentary" -> 1,
      "Drama" -> 2,
      "Fantasy" -> 1,
      "Film-Noir" -> 1,
      "Horror" -> 1,
      "Musical" -> 1,
      "Mystery" -> 1,
      "Romance" -> 1,
      "Sci-Fi" -> 32,
      "Thriller" -> 1,
      "War" -> 4,
      "Western" -> 1
    )

    val ratings = dataWithNoHeader.map(line => line.split(',') match {
      case Array(user, movieID, rating, date)
      => Rating(user.toInt, movieID.toInt, rating.toDouble)
    })

    val ratingsGroupByUser = ratings.groupBy(e => e.user).map(x =>
      (x._1, x._2.map(y => y.rating).seq))

    val userAverageRatingMap = ratingsGroupByUser.map(e => {
      (e._1, e._2.sum/e._2.size)
    }).collect().toMap

    val goodRatings = ratings.filter(e => e.rating >= userAverageRatingMap.get(e.user).get).sortBy(- _.rating)

    //Built movies: Map[Int, String]
    val moviesRaw = dataWithNoHeaderMovies
      .map(line =>
        line.replaceAll(", ", " ")
          .replaceAll("\"", "")
          .split(','))
    val movieNames: Map[Int, String] = moviesRaw.
      map(e => (e(0).toInt, e(1)))
      .collect()
      .toMap

    //Build Movie Genres
    var movieGenres = moviesRaw.map(e => {
      (e(0).toInt, e(2).split("\\|"))
    }).collect().toMap

    val userGenre = goodRatings
      .groupBy(e => e.user)
      .map(x => (x._1, x._2
                          .map(y => movieGenres.get(y.product)).flatten(z => z.get).filter(b => movieGenresData.contains(b))
                            .groupBy(identity).mapValues(a => a.size).map(identity)))

    val userVectors: RDD[(Int, Map[String, Int])] = userGenre.map(e => {
      var userGenres = e._2
      for(g <- movieGenresData) {
        if(!userGenres.contains(g))
          userGenres += (g -> 0)
      }
      (e._1, userGenres)
    })

    val recom = knn(testUser, movieNames, 10, userVectors, 25, goodRatings)
    recom.foreach(println)
  }

  def userSim(user1: Map[String, Int], user2: Map[String, Int]): Double = {
    val vectorsSum = user1.map(e => {
      e._2 * user2.get(e._1).get
    }).sum

    val length1 = user1.map(e =>
      e._2 * e._2
    ).sum

    val length2 = user2.map(e =>
      e._2 * e._2
    ).sum

    val similarity = vectorsSum / ((Math.sqrt(length1) * (Math.sqrt(length2))))

    similarity
  }

  def knn(testUser : Map[String, Int],
          movieMap : Map[Int, String],
          k : Int,
          userVectors: RDD[(Int, Map[String, Int])],
          numberOfMovies : Int,
          goodRatings : RDD[Rating]) : Seq[String] = {

    val userSimilarities = userVectors.map(user => {
      val userVector = user._2
      val similarity = userSim(testUser, userVector)
      (user._1, similarity)
    })

    val sortedSimilarities = userSimilarities.sortBy(- _._2)
    val mostSimilarUsers = sortedSimilarities.take(k)
    val userRatingMap = goodRatings
      .groupBy(e => e.user)
      .collect()
      .toMap

    val mostRatedMovies = mostSimilarUsers.map(user => {
      val userID = user._1
      val userSimilarity = user._2
      val userRatings = userRatingMap.get(userID).get.toSeq
      userRatings
    }).flatten.sortBy(- _.rating)

    val recom = mostRatedMovies
      .map(rating => movieMap.get(rating.product).get)
      .distinct
      .take(numberOfMovies)
    recom
  }

}
